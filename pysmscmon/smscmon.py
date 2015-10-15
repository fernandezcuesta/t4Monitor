#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
    start() ------------------.
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
    get_stats_from_host()

"""
from __future__ import absolute_import

import ConfigParser
import datetime as dt
import os
import Queue
import threading
from random import randint

import pandas as pd
from paramiko import SSHException

from . import calculations, df_tools
from .logger import init_logger
from .sshtunnels import sshtunnel
from .sshtunnels.sftpsession import SftpSession, SFTPSessionError

__all__ = ('add_methods_to_pandas_dataframe',
           'SMSCMonitor',
           'read_config')

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


class SMSCMonitor(object):

    """
    Defines class 'SMSCMonitor'
    """

    def __init__(self,
                 settings_file=None,
                 logger=None,
                 alldays=False,
                 nologs=False):
        self.alldays = alldays
        self.data = pd.DataFrame()
        self.conf = read_config(settings_file)
        self.logger = logger or init_logger()
        self.logs = {}
        self.nologs = nologs
        self.results_queue = Queue.Queue()
        self.settings_file = settings_file or DEFAULT_SETTINGS_FILE
        self.server = None
        self.systems = [item for item in self.conf.sections()
                        if item not in ['GATEWAY', 'MISC']]
        self.__str__()

    def __enter__(self):
        self.init_tunnels()
        self.start_server()
        return self

    def __exit__(self, etype, *args):
        self.stop_server()
        return None

    def consolidate_data(self, partial_dataframe):
        """
        Consolidates partial_dataframe with self.data by calling
        df_tools.consolidate_data
        """
        self.data = df_tools.consolidate_data(self.data, partial_dataframe)

    def clone(self):
        """ Makes a copy of SData
        """
        my_clone = SMSCMonitor()
        my_clone.alldays = self.alldays
        my_clone.conf = self.conf
        my_clone.data = self.data.copy()  # required in pandas
        my_clone.logger = self.logger
        my_clone.logs = self.logs
        my_clone.nologs = self.nologs
        my_clone.results_queue = Queue.Queue()  # make a brand new result queue
        my_clone.server = self.server
        my_clone.settings_file = self.settings_file
        return my_clone

    def init_tunnels(self, system=None):
        """
        Calls sshtunnel and returns a ssh server with all tunnels established
        server instance is returned non-started
        """
        self.logger.info('Initializing tunnels')
        if not self.conf:
            self.conf = read_config(self.settings_file)

        jumpbox_addr = self.conf.get('GATEWAY', 'ip_or_hostname')
        jumpbox_port = int(self.conf.get('GATEWAY', 'ssh_port'))
        rbal = []
        lbal = []
        tunnelports = {}

        for _sys in [system] if system else [x for x in self.conf.sections()
                                             if x not in ['GATEWAY', 'MISC']]:
            rbal.append((self.conf.get(_sys, 'ip_or_hostname'),
                         int(self.conf.get(_sys, 'ssh_port'))))
            lbal.append(('', int(self.conf.get(_sys, 'tunnel_port')) or
                         randint(61001, 65535)))  # if local port==0, random
            tunnelports[_sys] = lbal[-1][-1]
            self.conf.set(_sys, 'tunnel_port', str(tunnelports[_sys]))
        try:
            # Assert local tunnel ports are different
            assert len(tunnelports) == len(set(tunnelports.values()))
            pwd = self.conf.get('GATEWAY', 'password').strip("\"' ") or None \
                if self.conf.has_option('GATEWAY', 'password') else None
            pkey = self.conf.get('GATEWAY', 'identity_file').strip("\"' ") \
                or None if self.conf.has_option('GATEWAY', 'identity_file') \
                else None
            user = self.conf.get('GATEWAY', 'username') or None \
                if self.conf.has_option('GATEWAY', 'username') else None

            self.server = sshtunnel.SSHTunnelForwarder(
                ssh_address=jumpbox_addr,
                ssh_port=jumpbox_port,
                ssh_username=user,
                ssh_password=pwd,
                remote_bind_address_list=rbal,
                local_bind_address_list=lbal,
                threaded=False,
                logger=self.logger,
                ssh_private_key_file=pkey
                                                       )
            # Add the system<>port bindings to the return object
            self.server.tunnelports = tunnelports
            self.logger.debug('Registered tunnels: %s',
                              self.server.tunnelports)
        except AssertionError:
            self.logger.error('Local tunnel ports MUST be different: %s',
                              tunnelports)
            raise sshtunnel.BaseSSHTunnelForwarderError
        except sshtunnel.BaseSSHTunnelForwarderError:
            self.logger.error('%sCould not open connection to remote server: '
                              '%s:%s',
                              '%s | ' % system if system else '',
                              jumpbox_addr,
                              jumpbox_port)
            raise sshtunnel.BaseSSHTunnelForwarderError

    def collect_system_data(self, system):
        """ Open an sftp session to system and collects the CSVs, generating a
            pandas dataframe as outcome
        """
        if system not in self.conf.sections():
            return (None, None)
        self.logger.info('%s | Connecting to tunel port %s',
                         system,
                         self.server.tunnelports[system])

        ssh_pass = self.conf.get(system, 'password').strip("\"' ") or None \
            if self.conf.has_option(system, 'password') else None

        ssh_key = self.conf.get(system,
                                'identity_file').strip("\"' ") or None \
            if self.conf.has_option(system, 'identity_file') else None

        user = self.conf.get(system, 'username') or None \
            if self.conf.has_option(system, 'username') else None
        try:
            with SftpSession(hostname='127.0.0.1',
                             ssh_user=user,
                             ssh_pass=ssh_pass,
                             ssh_key=ssh_key,
                             ssh_timeout=self.conf.get(system, 'ssh_timeout'),
                             ssh_port=self.server.tunnelports[system],
                             logger=self.logger) as sftp_session:
                if not sftp_session:
                    raise SftpSession.Break  # break the with statement
                data = self.get_system_data(sftp_session, system)

                # Done gathering data, now get the logs
                if self.nologs or data.empty \
                   or not self.conf.has_option('MISC', 'smsc_log_cmd'):
                    logs = '{0} | Log collection omitted'.format(system)
                    self.logger.info(logs)
                else:
                    logs = self.get_system_logs(
                        sftp_session.ssh_transport,
                        system,
                        self.conf.get('MISC', 'smsc_log_cmd')
                                                ) \
                        or '{} | Missing logs!'.format(system)
            return (data, logs)
        except SFTPSessionError:
            return (None, None)

    def get_system_logs(self, ssh_session, system, log_cmd=None):
        """ Get log info from the remote system, assumes an already established
            ssh tunnel.
        """
        if not log_cmd:
            self.logger.error('No command was specified for log collection')
            return
        self.logger.info('Getting log output from %s (%s), may take a while.',
                         system,
                         log_cmd)
        try:  # ignoring stdin and stderr
            (_, stdout, _) = ssh_session.\
                             exec_command(log_cmd)
            return stdout.readlines()
        except Exception as _exc:
            self.logger.error('%s | Error occurred while getting logs: %s',
                              system, repr(_exc))
            return None

    def start_server(self):
        """
        Dummy function to start SSH servers
        """
        if not self.server:
            raise sshtunnel.BaseSSHTunnelForwarderError
        try:
            self.logger.info('Opening connection to gateway')
            self.server.start()
            if not self.server.is_started:
                raise sshtunnel.BaseSSHTunnelForwarderError(
                    "Couldn't start server"
                                                            )
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

    def stop_server(self):
        """
        Dummy function to stop SSH servers
        """
        try:
            if self.server and self.server.is_started:
                self.logger.info('Closing connection to gateway')
                self.server.stop()
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

    def get_system_data(self, session, system):
        """
        Create pandas DF from current session CSV files downloaded via SFTP
        """
        system_addr = self.conf.get(system, 'ip_or_hostname')
        data = pd.DataFrame()

        try:  # Test if destination folder is reachable
            destdir = self.conf.get(system, 'folder') or '.'
            session.chdir(destdir)
            self.logger.debug('%s | Changing to remote folder: %s',
                              system,
                              destdir)
            session.chdir()  # revert back to home folder
        except IOError:
            self.logger.error('%s | Directory "%s" not found at destination',
                              system,
                              self.conf.get(system, 'folder'))
            return data

        # filter remote files on extension and date
        # using MONTHS to avoid problems with locale rather than english
        # under windows environments
        if self.alldays:
            tag_list = ['.csv']
        else:
            tag_list = ['.csv', '%02i%s%i' % (dt.date.today().day,
                                              MONTHS[dt.date.today().month-1],
                                              dt.date.today().year)]
        try:  # if present, also filter on cluster id
            tag_list.append(self.conf.get(system, 'cluster_id').lower())
        except Exception:
            pass

        data = self.get_stats_from_host(hostname=system_addr,
                                        filespec_list=tag_list,
                                        sftp_session=session,
                                        files_folder=destdir)
        if data.empty:
            self.logger.warning('%s | Data size obtained is 0 Bytes, skipping '
                                'log collection.', system)

        else:
            self.logger.info('%s | Dataframe shape obtained: %s. '
                             'Now applying calculations...',
                             system, data.shape)
            calc_file = self.conf.get('MISC', 'calculations_file')
            if not os.path.isabs(calc_file):
                calc_file = '%s%s%s' % (os.path.dirname(os.path.abspath(
                                       self.settings_file)),
                                       os.sep,
                                       calc_file)
            data.apply_calcs(calc_file)
            self.logger.info('%s | Dataframe shape after calculations: %s',
                             system, data.shape)
        return data

    def get_stats_from_host(self, hostname=None, filespec_list=None, **kwargs):
        """
        Connects to a remote system via SFTP and reads the CSV files, then
        calls the csv-pandas conversion function.
        Working with local filesystem if hostname is None
        Returns: pandas dataframe

        **kwargs (optional):
        sftp_session: already established sftp session
        ssh_user, ssh_pass, ssh_pkey_file, ssh_configfile, ssh_port
        files_folder: folder where files are located, either on sftp srv or
                      local filesystem
        Otherwise: checks ~/.ssh/config
        """
        sftp_session = kwargs.pop('sftp_session', '')
        files_folder = kwargs.pop('files_folder', '.')
        if files_folder[-1] == os.sep:
            files_folder = files_folder[:-1]  # remove trailing separator (/)
        _df = pd.DataFrame()
        close_me = False
        filespec_list = filespec_list or ['.csv']  # default if no filter given
        if sftp_session:
            self.logger.debug('Using established sftp session...')
        else:
            if hostname:
                try:
                    sftp_session = SftpSession(hostname, **kwargs).connect()
                    if sftp_session:
                        close_me = True
                    else:
                        raise SFTPSessionError('connect failed')
                except SFTPSessionError as _exc:
                    self.logger.error('Error occurred while SFTP session '
                                      'to %s: %s', hostname, _exc)
                    return _df
            else:
                self.logger.info('Using local filesystem to get the files')
        filesource = sftp_session if sftp_session else os
        # get file list by filtering with taglist (case insensitive)
        try:
            filesource.chdir(files_folder)
            files = ['{}/{}'.format(filesource.getcwd(), f)
                     for f in filesource.listdir('.')
                     if all([val.upper() in f.upper()
                             for val in filespec_list])]
            if not files and not hostname:
                files = [filespec_list]  # For absolute paths
        except OSError:
            files = [filespec_list]  # When localfs, don't behave as filter
        if not files:
            self.logger.debug('Nothing gathered from %s, no files were '
                              'selected', hostname or 'local system')
            return _df
        _df = pd.concat([df_tools.dataframize(
            a_file,
            sftp_session,
            self.logger) for a_file in files], axis=0)
        if close_me:
            self.logger.debug('Closing sftp session')
            sftp_session.close()
        # calling df_tools.consolidate_data instead of self method to avoid
        # concatenation with self.data
        return df_tools.consolidate_data(_df)

    def thread_wrapper(self, system):
        """
        Thread method for main_threaded
        """
        # Get data from the remote system
        try:
            self.logger.info('%s | Collecting statistics...', system)
            tunnelport = self.server.tunnelports[system]
            if not self.server.tunnel_is_up[tunnelport]:
                self.logger.error('%s | System not reachable!', system)
                raise IOError
            (data, log) = self.collect_system_data(system)
            # self.logger.debug('%s | Putting results in queue', system)
        except (IOError, SFTPSessionError):
            data = pd.DataFrame()
            log = 'Could not get information from this system'
        # self.results_queue.put((system, data, log))
        self.logger.debug('%s | Consolidating results', system)
        self.consolidate_data(data)
        self.logs[system] = log
        self.results_queue.put(system)

    def main_threads(self):
        """ Threaded method for main() """
        self.init_tunnels()
        self.start_server()
        for system in self.systems:
            thread = threading.Thread(target=self.thread_wrapper,
                                      name=system,
                                      args=(system, ))
            thread.daemon = True
            thread.start()
# TODO: get rid of this for loop?? No, it makes sense to wait for threads
        for system in self.systems:
            self.logger.info('%s | Done collecting data!',
                             self.results_queue.get())
            # (system, res_data, res_log) = self.results_queue.get()
            # self.logger.debug('%s | Consolidating results', system)
            # self.consolidate_data(res_data)
            # self.logs[system] = res_log
            # self.logger.info('%s | Done collecting data!', system)
            # self.server.stop()

    def main_no_threads(self):
        """ Serial (legacy) method for main() """
        for system in self.systems:
            self.logger.info('%s | Initializing tunnel', system)
            try:
                self.init_tunnels(system=system)
                self.start_server()
                tunnelport = self.server.tunnelports[system]
                if tunnelport not in self.server.tunnel_is_up or \
                   not self.server.tunnel_is_up[tunnelport]:
                    self.logger.error('Cannot download data from %s.', system)
                    raise IOError
                res_data, self.logs[system] = self.collect_system_data(system)
                self.consolidate_data(res_data)
                self.logger.info('Done for %s', system)
                self.server.stop()
            except (sshtunnel.BaseSSHTunnelForwarderError,
                    IOError,
                    SFTPSessionError):
                # _sd.server.stop()
                self.logger.warning('Continue to next system')
                continue

    def start(self, threads=None):
        """
        Main method for SMSCMonitor class
        """
        try:
            if threads:
                self.main_threads()
            else:
                self.main_no_threads()
        except (sshtunnel.BaseSSHTunnelForwarderError, AttributeError) as exc:
            self.logger.error('Could not initialize the SSH tunnels, '
                              'aborting (%s)', repr(exc))
        except SSHException:
            self.logger.error('Could not open remote connection')
        except Exception as exc:
            self.logger.error('Unexpected error: %s)', repr(exc))

    def __str__(self):
        return 'alldays/nologs: {}/{}\ndata shape:{}\nlogs (keys): {}' \
               'server is set up?: {}\n' \
               'Settings file: {}'.format(self.alldays,
                                          self.nologs,
                                          self.data.shape,
                                          self.logs.keys(),
                                          'Yes' if self.server else 'No',
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


def add_methods_to_pandas_dataframe(logger=None):
    """ Add custom methods to pandas.DataFrame """
    pd.DataFrame._metadata = ['system']  # default metadata
    pd.DataFrame.__finalize__ = _custom_finalize
    pd.DataFrame.to_pickle = pd.to_pickle = df_tools.to_pickle
    pd.DataFrame.read_pickle = pd.read_pickle = df_tools.read_pickle
    pd.DataFrame.oper = calculations.oper
    pd.DataFrame.oper_wrapper = calculations.oper_wrapper
    pd.DataFrame.recursive_lis = calculations.recursive_lis
    pd.DataFrame.apply_calcs = calculations.apply_calcs
    pd.DataFrame.clean_calcs = calculations.clean_calcs
    pd.DataFrame.logger = logger or init_logger()
# END OF ADD METHODS TO PANDAS DATAFRAME


def read_config(settings_file=None):
    """ Return ConfigParser object from configuration file """
    config = ConfigParser.SafeConfigParser()
    try:
        settings_file = settings_file or DEFAULT_SETTINGS_FILE
        settings = config.read(settings_file)
    except ConfigParser.Error as _exc:
        raise ConfigReadError(repr(_exc))

    if not settings or not config.sections():
        raise ConfigReadError('Could not read configuration %s!' %
                              settings_file)
    return config


def main(alldays=False, nologs=False, logger=None, threads=False,
         settings_file=None):
    """ Here comes the main function
    Optional: alldays (Boolean): if true, do not filter on today's date
              nologs (Boolean): if true, skip log info collection
    """
    # TODO: review exceptions and comment where they may come from
    try:
        # Initialize monitor
        collector = SMSCMonitor(settings_file, logger, alldays, nologs)
        add_methods_to_pandas_dataframe(collector.logger)
        # Collect all the data and logs
        collector.start(threads)

    except ConfigReadError:
        collector.logger.error('Could not read settings file: %s',
                               collector.settings_file)

    return collector.data, collector.logs
