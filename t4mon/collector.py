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

import os
import Queue
import datetime as dt
import threading
import ConfigParser
from random import randint

import pandas as pd
from paramiko import SSHException
import sshtunnel

from . import df_tools, calculations
from .logger import init_logger
from sshtunnels.sftpsession import SftpSession, SFTPSessionError

__all__ = ('add_methods_to_pandas_dataframe',
           'Collector',
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


class Collector(object):

    """
    Defines the main Collector class for carrying the data, logs and additional
    options
    """

    def __init__(self,
                 alldays=False,
                 logger=None,
                 nologs=False,
                 settings_file=None,
                 safe=False,
                 **kwargs):

        self.alldays = alldays
        self.data = pd.DataFrame()
        self.conf = read_config(settings_file)
        self.logger = logger or init_logger()
        self.logs = {}
        self.nologs = nologs
        self.results_queue = Queue.Queue()
        self.settings_file = settings_file or DEFAULT_SETTINGS_FILE
        self.safe = safe
        self.server = None
        self.systems = [item for item in self.conf.sections()
                        if item not in ['GATEWAY', 'MISC']]
        self.__str__()
        add_methods_to_pandas_dataframe(self.logger)

    def __enter__(self, system=None):
        self.init_tunnels(system)
        self.start_server()
        return self

    def __exit__(self, etype, *args):
        self.stop_server()
        return None

    def __str__(self):
        return ('alldays/nologs: {0}/{1}\ndata shape:{2}\nlogs (keys): {3}\n'
                'threaded:{4}\nserver is set up?: {5}\n'
                'Settings file: {6}'.format(self.alldays,
                                            self.nologs,
                                            self.data.shape,
                                            self.logs.keys(),
                                            not self.safe,
                                            'Yes' if self.server else 'No',
                                            self.settings_file))

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

        for _sys in [system] if system else self.systems:
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
                ssh_address_or_host=(jumpbox_addr, jumpbox_port),
                ssh_username=user,
                ssh_password=pwd,
                remote_bind_addresses=rbal,
                local_bind_addresses=lbal,
                threaded=False,
                logger=self.logger,
                ssh_private_key=pkey
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

    def start_server(self):
        """
        Dummy function to start SSH servers
        """
        if not self.server:
            raise sshtunnel.BaseSSHTunnelForwarderError
        try:
            self.logger.info('Opening connection to gateway')
            self.server.start()
            if not self.server._is_started:
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
            if self.server and self.server._is_started:
                self.logger.info('Closing connection to gateway')
                self.server.stop()
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

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
                   or not self.conf.has_option('MISC', 'remote_log_cmd'):
                    logs = '{0} | Log collection omitted'.format(system)
                    self.logger.info(logs)
                else:
                    logs = self.get_system_logs(
                        sftp_session.ssh_transport,
                        system,
                        self.conf.get('MISC', 'remote_log_cmd')) or \
                        '{} | Missing logs!'.format(system)
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
        # under Windows environments
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
        # data.system = [system]
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
            data.apply_calcs(calc_file, system)
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
        _df = pd.concat([df_tools.dataframize(a_file,
                                              sftp_session,
                                              self.logger)
                         for a_file in files], axis=0)
        _df = df_tools.remove_dataframe_holes(_df)
        # for a_file in files:
        #     _df = _df.combine_first(df_tools.dataframize(a_file,
        #                                                  sftp_session,
        #                                                  self.logger))
        if close_me:
            self.logger.debug('Closing sftp session')
            sftp_session.close()
        return _df

    def check_if_tunnel_is_up(self, system):
        """ Return true if there's a tuple in self.server.tunnel_is_up such as:
            {('0.0.0.0', port): True} where port is the tunnelport for system
        """
        port = self.server.tunnelports[system]
        return any(port in address_tuple for address_tuple
                   in self.server.tunnel_is_up.iterkeys()
                   if self.server.tunnel_is_up[address_tuple])

    def thread_wrapper(self, system):
        """
        Thread wrapper method for main_threads
        """
        # Get data from the remote system
        try:
            self.logger.info('%s | Collecting statistics...', system)
            if not self.check_if_tunnel_is_up(system):
                self.logger.error('%s | System not reachable!', system)
                raise IOError
            (result_data, result_log) = self.collect_system_data(system)
            # self.logger.debug('%s | Putting results in queue', system)
        except (IOError, SFTPSessionError):
            result_data = pd.DataFrame()
            result_log = 'Could not get information from this system'
        # self.results_queue.put((system, data, log))
        self.logger.debug('%s | Consolidating results', system)
        self.data = df_tools.consolidate_data(result_data, self.data, system)
        self.logs[system] = result_log
        self.results_queue.put(system)

    def main_threads(self):
        """ Threaded method for main() """
        with self:
            for system in self.systems:
                thread = threading.Thread(target=self.thread_wrapper,
                                          name=system,
                                          args=(system, ))
                thread.daemon = True
                thread.start()
            # wait for threads to end, first one to finish will leave
            # the result in the queue
            for system in self.systems:
                self.logger.info('%s | Done collecting data!',
                                 self.results_queue.get())

    def main_no_threads(self):
        """ Serial (legacy) method for main() """
        for system in self.systems:
            self.logger.info('%s | Initializing tunnel', system)
            try:
                self.init_tunnels(system)
                self.start_server()
                self.thread_wrapper(system)

                # if not self.check_if_tunnel_is_up(system):
                #     self.logger.error('%s | System not reachable!', system)
                #     raise IOError
                # (result_data,
                #  self.logs[system]) = self.collect_system_data(system)
                # consolidate_data(self.data, result_data, system)
                # self.logger.info('%s | Done collecting data!', system)

                self.stop_server()
            except (sshtunnel.BaseSSHTunnelForwarderError,
                    IOError,
                    SFTPSessionError):
                self.logger.warning('Continue to next system (if any)')
                continue

    def start(self):
        """
        Main method for the data collection
        """
        try:
            if self.safe:
                self.main_no_threads()
            else:
                self.main_threads()
        except (sshtunnel.BaseSSHTunnelForwarderError, AttributeError) as exc:
            self.logger.error('Could not initialize the SSH tunnels, '
                              'aborting (%s)', repr(exc))
        except SSHException:
            self.logger.error('Could not open remote connection')
        except Exception as exc:
            self.logger.error('Unexpected error: %s)', repr(exc))
        finally:
            return (self.data, self.logs)


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


# ADD METHODS TO PANDAS DATAFRAME
# def _custom_finalize(self, other, method=None):
#     """ As explained in http://stackoverflow.com/questions/23200524/
#         propagate-pandas-series-metadata-through-joins
#         => Custom __finalize__ function for concat, so we keep the metadata
#     """
#     def _wrapper(element, name):
#         """ Wrapper for map function """
#         _cur_meta = getattr(self, name, '')
#         _el_meta = getattr(element, name, '')
#         if _cur_meta:
#             if isinstance(_el_meta, set):
#                 setattr(self, name, _cur_meta.union(_el_meta))
#             else:
#                 _cur_meta.add(_el_meta)
#         else:
#             setattr(self,
#                     name,
#                     _el_meta if isinstance(_el_meta, set) else set([_el_meta]))
#     for name in self._metadata:
#         if method == 'concat':
#             # map(lambda element: _wrapper(element, name), other.objs)
#             [_wrapper(element, name) for element in other.objs]
#         else:
#             setattr(self, name, getattr(other, name, ''))
#     return self


def add_methods_to_pandas_dataframe(logger=None):
    """ Add custom methods to pandas.DataFrame """
    pd.DataFrame._metadata = ['system']  # default metadata
    # pd.DataFrame.__finalize__ = _custom_finalize
    pd.DataFrame.to_pickle = pd.to_pickle = df_tools.to_pickle
    pd.DataFrame.read_pickle = pd.read_pickle = df_tools.read_pickle
    pd.DataFrame.oper = calculations.oper
    pd.DataFrame.oper_wrapper = calculations.oper_wrapper
    pd.DataFrame.recursive_lis = calculations.recursive_lis
    pd.DataFrame.apply_calcs = calculations.apply_calcs
    pd.DataFrame.clean_calcs = calculations.clean_calcs
    pd.DataFrame.logger = logger or init_logger()
# END OF ADD METHODS TO PANDAS DATAFRAME
