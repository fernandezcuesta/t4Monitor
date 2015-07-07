#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 SMSCMon: T4-compliant CSV processor and visualizer for Acision SMSC Monitor
 ------------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 t4 input_file

 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information.

  line1: Collection date  (optional line)

  line2: Start time       (optional line)

  line3: Parameter Headings (comma separated).

 or

  ** Format 2: **

 line0: Header information containing T4 revision info and system information.
 line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple $
 line2: parameter headings (comma separated)
 ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple $

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing hash
  and contents followed by it (sometimes even more samples...) is ignored.




    main() ------------------.
     |                       | no threads (legacy serial mode)
     v                       |
    thread_wrapper()         |
     |                       |
     v                       |
    collect_system_data()  <-'
     |                \
     v                 \
    get_system_data()   `---> get_system_logs()
     |
     v
    get_stats_from_hosts()
Created on Mon May 25 11:11:38 2015

"""
from __future__ import absolute_import

import os
import ConfigParser
import threading
import datetime as dt
import Queue
import getpass

import pandas as pd
from random import randint
from paramiko import SSHException

from . import df_tools
from . import calculations
from .logger import init_logger
from .sshtunnels import sshtunnel
from .sshtunnels.sftpsession import SftpSession, SFTPSessionError


__all__ = ('main', 'collect_system_data',
           'get_stats_from_host', 'init_tunnels')

# CONSTANTS
DEFAULT_SETTINGS_FILE = '{}/conf/settings.cfg'.\
                        format(os.path.dirname(os.path.abspath(__file__)))
# Avoid using locale in Linux+Windows environments, keep these lowercase
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
LINE = 80 * '-'


# CLASSES

class ConfigReadError(Exception):

    """Exception raised while reading configuration file"""
    pass


class InitTunnelError(Exception):

    """Exception raised by InitTunnel"""
    pass


class NoFileFound(Exception):

    """Exception raised by get_stats_from_host"""
    pass


class SData(object):

    """
    Defines class 'SData' for use in `get_system_data` and `thread_wrapper`
    """

    def __init__(self):
        self.server = None
        self.system = ''
        self.conf = None
        self.alldays = False
        self.nologs = False
        self.logger = None
        self.settings_file = None

    def clone(self, system):
        """ Makes a copy of SData
        """
        my_clone = SData()
        my_clone.server = self.server
        my_clone.system = system
        my_clone.conf = self.conf
        my_clone.alldays = self.alldays
        my_clone.nologs = self.nologs
        my_clone.logger = self.logger
        my_clone.settings_file = self.settings_file
        return my_clone

    def __str__(self):
        return 'System: {0}\nalldays/nologs: {1}/{2}\n' \
               'Settings file: {3}'.format(self.system,
                                           self.alldays,
                                           self.nologs,
                                           self.settings_file)


# ADD METHODS TO PANDAS DATAFRAME
def _custom_finalize(self, other, method=None):
    """ As explained in http://stackoverflow.com/questions/23200524/
        propagate-pandas-series-metadata-through-joins
        => Custom __finalize__ function for concat, so we keep the metadata
    """
    def _wrapper(element, name):
        """ Wrapper for map function """
        _cur_meta = getattr(self, name, '')
        _el_meta = getattr(element, name, '')
        if _cur_meta:
            if isinstance(_el_meta, set):
                setattr(self, name, _cur_meta.union(_el_meta))
            else:
                _cur_meta.add(_el_meta)
        else:
            setattr(self,
                    name,
                    _el_meta if isinstance(_el_meta, set) else set([_el_meta]))
    for name in self._metadata:
        if method == 'concat':
            # map(lambda element: _wrapper(element, name), other.objs)
            [_wrapper(element, name) for element in other.objs]
        else:
            setattr(self, name, getattr(other, name, ''))
    return self


pd.DataFrame._metadata = ['system']  # default metadata
pd.DataFrame.__finalize__ = _custom_finalize
pd.DataFrame.to_pickle = pd.to_pickle = df_tools.to_pickle
pd.DataFrame.read_pickle = pd.read_pickle = df_tools.read_pickle
pd.DataFrame.oper = calculations.oper
pd.DataFrame.oper_wrapper = calculations.oper_wrapper
pd.DataFrame.recursive_lis = calculations.recursive_lis
pd.DataFrame.apply_calcs = calculations.apply_calcs
pd.DataFrame.clean_calcs = calculations.clean_calculations
# pd.DataFrame.logger = init_logger()
# END OF ADD METHODS TO PANDAS DATAFRAME


def read_config(*settings_file):
    """ Return ConfigParser object from configuration file """
    config = ConfigParser.SafeConfigParser()
    try:
        settings = config.read(settings_file) if settings_file \
                   else config.read(DEFAULT_SETTINGS_FILE)
    except ConfigParser.Error as _exc:
        raise ConfigReadError(repr(_exc))

    if not settings or not config.sections():
        raise ConfigReadError('Could not read configuration %s!' %
                              settings_file if settings_file
                              else DEFAULT_SETTINGS_FILE)
    # If no 'username' was set in DEFAULT, override with current user's
    try:
        username = config.get('DEFAULT', 'username')
        if not username:
            raise ConfigParser.NoOptionError()
    except ConfigParser.NoOptionError:
        config.set('DEFAULT', 'username', getpass.getuser())
    return config


def init_tunnels(config, logger, system=None):
    """
    Calls sshtunnel and returns a ssh server with all tunnels established
    server instance is returned non-started
    """
    logger.info('Initializing tunnels')
    if not config:
        config = read_config()

    jumpbox_addr = config.get('GATEWAY', 'ip_or_hostname')
    jumpbox_port = int(config.get('GATEWAY', 'ssh_port'))
    rbal = []
    lbal = []
    tunnelports = {}

    for _sys in [system] if system else [x for x in config.sections()
                                         if x not in ['GATEWAY', 'MISC']]:
        rbal.append((config.get(_sys, 'ip_or_hostname'),
                     int(config.get(_sys, 'ssh_port'))))
        lbal.append(('', int(config.get(_sys, 'tunnel_port')) or
                     randint(61001, 65535)))  # if local port==0, random port
        tunnelports[_sys] = lbal[-1][-1]
        config.set(_sys, 'tunnel_port', str(tunnelports[_sys]))
    try:
        # Assert local tunnel ports are different
        assert len(tunnelports) == len(set([tunnelports[k]
                                            for k in tunnelports]))
        pwd = config.get('GATEWAY', 'password').strip("\"' ") or None \
            if config.has_option('GATEWAY', 'password') else None
        pkey = config.get('GATEWAY', 'identity_file').strip("\"' ") or None \
            if config.has_option('GATEWAY', 'identity_file') else None

        server = sshtunnel.SSHTunnelForwarder(ssh_address=jumpbox_addr,
                                              ssh_port=jumpbox_port,
                                              ssh_username=config.
                                              get('GATEWAY', 'username'),
                                              ssh_password=pwd,
                                              remote_bind_address_list=rbal,
                                              local_bind_address_list=lbal,
                                              threaded=False,
                                              logger=logger,
                                              ssh_private_key_file=pkey
                                              )
        # Add the system<>port bindings to the return object
        server.tunnelports = tunnelports
        return server
    except AssertionError:
        logger.error('Local tunnel ports MUST be different: %s', tunnelports)
        raise sshtunnel.BaseSSHTunnelForwarderError
    except sshtunnel.BaseSSHTunnelForwarderError:
        logger.error('%sCould not open connection to remote server: %s:%s',
                     '%s| ' % system if system else '',
                     jumpbox_addr,
                     jumpbox_port)
        raise sshtunnel.BaseSSHTunnelForwarderError


def thread_wrapper(sdata, results_queue, system):
    """
    Wrapper function for main_threaded
    """
    # Get data from the remote system
    try:
        thread_sdata = sdata.clone(system)
        thread_sdata.logger.info('%s| Collecting statistics...', system)
        tunnelport = thread_sdata.server.tunnelports[system]
        if not thread_sdata.server.tunnel_is_up[tunnelport]:
            thread_sdata.logger.error('%s| System not reachable!', system)
            raise IOError
        data, log = collect_system_data(thread_sdata)
        thread_sdata.logger.debug('%s| Putting results in queue', system)
    except (IOError, SFTPSessionError):
        data = pd.DataFrame()
        log = 'Could not get information from this system'
    results_queue.put((system, data, log))


def collect_system_data(sdata):
    """ Open an sftp session to system and collects the CSVs, generating a
        pandas dataframe as outcome
    """
    data = pd.DataFrame()
    logger = sdata.logger or init_logger()
    tunn_port = sdata.conf.get(sdata.system, 'tunnel_port')
    logger.info('%s| Connecting to tunel port %s', sdata.system, tunn_port)

    ssh_pass = sdata.conf.get(sdata.system, 'password').strip("\"' ") or None \
        if sdata.conf.has_option(sdata.system, 'password') else None
    ssh_key = sdata.conf.get(sdata.system, 'identity_file').strip("\"' ") \
        or None if sdata.conf.has_option(sdata.system, 'identity_file') \
        else None

    with SftpSession(hostname='127.0.0.1',
                     ssh_user=sdata.conf.get(sdata.system, 'username'),
                     ssh_pass=ssh_pass,
                     ssh_key=ssh_key,
                     ssh_timeout=sdata.conf.get(sdata.system, 'ssh_timeout'),
                     ssh_port=tunn_port, logger=logger) as session:
        sftp_session = session.sftp_session

        if not sftp_session:
            raise SftpSession.Break  # break the with statement
        data = get_system_data(sdata, sftp_session)

        # Done gathering data, now get the logs
        if sdata.nologs or not sdata.conf.has_option('MISC', 'smsc_log_cmd'):
            logs = '{0}| Log collection omitted'.format(sdata.system)
            logger.info(logs)
        else:
            logs = get_system_logs(session,
                                   sdata.system,
                                   sdata.conf.get('MISC', 'smsc_log_cmd'),
                                   logger) \
                   or '{}| Missing logs!'.format(sdata.system)
    return data, logs


def get_system_logs(session, system, log_cmd=None, logger=None):
    """ Get log info from the remote system, assumes an already established
        ssh tunnel.
    """
    logger = logger or logger.init_logger()
    if not log_cmd:
        logger.error('No command was specified for log collection')
        return
    logger.info('Getting log output from %s (%s), may take a while...',
                system,
                log_cmd)
    try:  # ignoring stdin and stderr
        (_, stdout, _) = session.\
                         exec_command(log_cmd)
#       #remove carriage returns ('\r\n') from the obtained lines
#       logs = [_con for _con in \
#              [_lin.strip() for _lin in stdout.readlines()] if _con]
        return stdout.readlines()  # [_lin for _lin in stdout.readlines()]
    except Exception as _exc:
        logger.error('%s| Error occurred while getting logs: %s',
                     system, repr(_exc))
        return None


def get_system_data(sdata, session):
    """ Create pandas DF from current session CSV files downloaded via SFTP """
    logger = sdata.logger or init_logger()
    system_addr = sdata.conf.get(sdata.system, 'ip_or_hostname')
    data = pd.DataFrame()

    try:
        session.chdir(sdata.conf.get(sdata.system, 'folder'))
    except IOError:
        logger.error('%s| %s not found at destination',
                     sdata.system,
                     sdata.conf.get(sdata.system, 'folder'))
        raise IOError

    # filter remote files on extension and date
    # using MONTHS to avoid problems with locale rather than english
    # under windows environments
    if sdata.alldays:
        tag_list = ['.csv']
    else:
        tag_list = ['.csv', '%02i%s%i' % (dt.date.today().day,
                                          MONTHS[dt.date.today().month - 1],
                                          dt.date.today().year)]
    try:  # if present, also filter on cluster id
        tag_list.append(sdata.conf.get(sdata.system, 'cluster_id').lower())
    except Exception:
        pass

    data = get_stats_from_host(system_addr,
                               tag_list,
                               sftp_client=session,
                               logger=logger,
                               sftp_folder=sdata.conf.get(sdata.system,
                                                          'folder'))
    if data.empty:
        logger.warning('%s| Data size obtained is 0 Bytes, skipping '
                       'log collection.', sdata.system)
        setattr(sdata, 'nologs', True)

    else:
        logger.info('%s| Dataframe shape obtained: %s. '
                    'Now applying calculations...',
                    sdata.system, data.shape)
        calc_file = sdata.conf.get('MISC', 'calculations_file')
        if not os.path.isabs(calc_file):
            calc_file = '%s/%s' % (os.path.dirname(os.path.abspath(\
                                   sdata.settings_file)),
                                   calc_file)
        data.apply_calcs(calc_file)
        logger.info('%s| Dataframe shape after calculations: %s',
                    sdata.system, data.shape)

    return data


def get_stats_from_host(hostname, tag_list, **kwargs):
    """
    Connects to a remote system via SFTP and reads the CSV files, then calls
    the csv-pandas conversion function.
    Working with local filesystem if hostname == 'localfs'
    Returns: pandas dataframe

    **kwargs (optional):
    sftp_client: already established sftp session
    logger: logging.Logger instance
    ssh_user, ssh_pass, ssh_pkey_file, ssh_configfile, ssh_port
    files_folder: folder where files are located, either on sftp srv or localfs
    Otherwise: checks ~/.ssh/config
    """
    logger = kwargs.get('logger', '') or init_logger()
    sftp_session = kwargs.pop('sftp_client', '')
    files_folder = kwargs.pop('files_folder', './')
    _df = pd.DataFrame()
    close_me = False

    try:
        if not sftp_session:
            if hostname == 'localfs':
                logger.info('Using local filesystem to get the files')
            else:
                session = SftpSession(hostname, **kwargs).connect()
                if not session:
                    logger.debug('Could not establish an SFTP session to %s',
                                 hostname)
                    return pd.DataFrame()
                sftp_session = session.sftp_session
                close_me = True
        else:
            logger.debug('Using established sftp session...')

        if not sftp_session:
            filesystem = os  # for localfs mode
        else:
            filesystem = sftp_session

        # get file list by filtering with taglist (case insensitive)
        try:
            files = ['{}{}'.format(files_folder, f)
                     for f in filesystem.listdir(files_folder) if
                     all([val.upper() in f.upper() for val in tag_list])]
            if not files:
                files = [tag_list]  # For absolute paths
        except OSError:
            files = [tag_list]  # When using localfs, specify instead of filter
        if not files:
            logger.debug('Nothing gathered from %s, no files were selected',
                         hostname)
            return _df
        _df = pd.concat([df_tools.dataframize(a_file, sftp_session, logger)
                         for a_file in files], axis=0)
        if close_me:
            logger.debug('Closing sftp session')
            session.close()

#        _tmp_df = df_tools.copy_metadata(_df)
#       properly merge the columns and restore the metadata
#        _df = _df.groupby(_df.index).last()
#        df_tools.restore_metadata(_tmp_df, _df)

        _df = df_tools.consolidate_data(_df)

    except SFTPSessionError as _exc:
        logger.error('Error occurred while SFTP session to %s: %s',
                     hostname,
                     _exc)
#    except Exception:
#        exc_typ, _, exc_tb = sys.exc_info()
#        print 'Unexpected error ({2}@line {0}): {1}'.format(exc_tb.tb_lineno,
#                                 exc_typ, exc_tb.tb_frame.f_code.co_filename)
    return _df


def start_server(server, logger):
    """
    Dummy function to start SSH servers
    """
    if not server:  # or not _sd.server.is_started:
        raise sshtunnel.BaseSSHTunnelForwarderError
    logger.info('Opening connection to gateway')
    server.start()
    if not server.is_started:
        raise sshtunnel.BaseSSHTunnelForwarderError


def main(alldays=False, nologs=False, logger=None, threads=False,
         settings_file=None):
    """ Here comes the main function
    Optional: alldays (Boolean): if true, do not filter on today's date
              nologs (Boolean): if true, skip log info collection
    """
    # TODO: review exceptions and comment where they may come from
    # logger = logger or logger.init_logger()
    data = pd.DataFrame()
    logs = {}

    _sd = SData()
    _sd.logger = logger or logger.init_logger()
    _sd.alldays = alldays
    _sd.nologs = nologs
    _sd.settings_file = settings_file
    # setattr(_sd, 'logger', logger)
    # setattr(_sd, 'alldays', alldays)
    # setattr(_sd, 'nologs', nologs)

    try:
        setattr(_sd, 'conf', read_config())
        all_systems = [item for item in _sd.conf.sections()
                       if item not in ['GATEWAY', 'MISC']]
        if threads:
            setattr(_sd, 'server', init_tunnels(_sd.conf, logger))
            start_server(_sd.server, logger)
            results_queue = Queue.Queue()
            for item in [threading.Thread(target=thread_wrapper,
                                          name=sys,
                                          args=(_sd, results_queue, sys))
                         for sys in all_systems]:
                item.daemon = True
                item.start()

            for item in range(len(all_systems)):
                sys, res_data, res_log = results_queue.get()
                _sd.logger.debug('%s| Consolidating results', sys)
                data = df_tools.consolidate_data(data, res_data)
                logs[sys] = res_log
                _sd.logger.info('%s| Done collecting data!', sys)
                _sd.server.stop()
        else:
            for _sd.system in all_systems:
                _sd.logger.info('%s| Initializing tunnel', _sd.system)
                try:
                    setattr(_sd, 'server', init_tunnels(_sd.conf,
                                                        logger,
                                                        _sd.system))
                    start_server(_sd.server, logger)
                    tunnelport = _sd.server.tunnelports[_sd.system]
                    if tunnelport not in _sd.server.tunnel_is_up or \
                    not _sd.server.tunnel_is_up[tunnelport]:
                        _sd.logger.error('Cannot download data from %s.',
                                         _sd.system)
                        raise IOError
                    res_data, logs[_sd.system] = collect_system_data(_sd)
                    data = df_tools.consolidate_data(data, res_data)
                    _sd.logger.info('Done for %s', _sd.system)
                    _sd.server.stop()
                except (sshtunnel.BaseSSHTunnelForwarderError,
                        IOError,
                        SFTPSessionError):
                    # _sd.server.stop()
                    _sd.logger.warning('Continue to next system')
                    continue

    except (sshtunnel.BaseSSHTunnelForwarderError, AttributeError) as exc:
        _sd.logger.error('Could not initialize the SSH tunnels, aborting (%s)',
                         repr(exc))
    except ConfigReadError:
        _sd.logger.error('Could not read settings file: %s', _sd.settings_file)
    except SSHException:
        _sd.logger.error('Could not open remote connection')
    except Exception as exc:
        _sd.logger.error('Unexpected error: %s)',
                         repr(exc))

    return data, logs
