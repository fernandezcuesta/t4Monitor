#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
    start() ------------------.
     |                       | no threads (legacy serial mode)
     v                       |
    collect_system_data()    |
     |                       |
     v                       |
    get_data_and_logs()    <-'
     |                \
     v                 \
    get_system_data()   `---> get_system_logs()
     |
     v
    get_stats_from_host()

"""
from __future__ import absolute_import

import copy
import os
import gzip
import Queue
import datetime as dt
import tempfile
import threading
import zipfile
import __builtin__
import ConfigParser
from contextlib import contextmanager
from cStringIO import StringIO
from paramiko import SFTPClient
from random import randint

import pandas as pd
import sshtunnel
from paramiko import SSHException
from sshtunnels.sftpsession import SftpSession, SFTPSessionError

from . import df_tools, calculations, gen_plot
from .logger import init_logger

try:
    import cPickle as pickle
except ImportError:
    import pickle

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


@contextmanager
def change_dir(directory, module):
    """ Context manager for restoring the current working directory """
    module = module or os
    current_dir = module.getcwd()
    module.chdir(directory)
    yield
    module.chdir(current_dir)


class ConfigReadError(Exception):

    """Exception subclass raised while reading configuration file"""
    pass


class Collector(object):

    """
    Main data collection class
    Takes care of:

    - Initialize the SSH tunnels towards the remote clusters over a common
      gateway
    - Collecting the data (T4-CSV) from remote clusters
    - Optionally collect remote command output
    - Create a dataframe containing processed data from all systems
    - Apply calculations to resulting dataframe, saving the result in new
      columns

    Additional methods allow storing/loading the class into a gzipped pickle
    file.

    Modes:
        - threaded: default mode, most operations are executed in parallel for
                    each system
        - safe: serial mode, slower. All operations are executed system by
                system

    Class __init__ arguments:
        - alldays
          Type: boolean
          Default: False
          Description: Define whether or not filter remote files on current
                       date. If true, remote files will be filtered on a
                       timestamp DDMMMYYY (i.e. '20may2015')
        - logger
          Type: logging.Logger
          Default: None
          Description: Logger object passed from an external function.
                       A new logger is created by calling logger.init_logger()
                       if nothing is passed
        - nologs
          Type: boolean
          Default: False
          Description: Skip remote log collection. An indication message will
                       be shown in the report showing that the log collection
                       was omitted
        - safe
          Type: boolean
          Default: False
          Description: Define the mode (safe or threaded) for most of the class
                       methods
        - settings_file
          Type: string
          Default: DEFAULT_SETTINGS_FILE
          Description: Define the name of the configuration file


      Class objects:

          - All class __init__ arguments as defined above
          - logs
              Type: dict
          - results_queue
          - server
          - systems
      Usage:

          with Collector(**options) as col:
              ...
                  operations
              ...

       or

           col = Collector(**options)
           col.init_tunnels()
           ...
               operations
           ...
           col.stop_server()


    """

    def __init__(self,
                 alldays=False,
                 logger=None,
                 nologs=False,
                 safe=False,
                 settings_file=None,
                 **kwargs):

        self.alldays = alldays
        self.data = pd.DataFrame()
        self.conf = read_config(settings_file)
        self.logger = logger or init_logger()
        self.logs = {}
        self.nologs = nologs
        self.results_queue = Queue.Queue()
        self.safe = safe
        self.settings_file = settings_file or DEFAULT_SETTINGS_FILE
        self.server = None
        self.systems = [item for item in self.conf.sections()
                        if item not in ['GATEWAY', 'MISC']]
        self.use_gateway = self.conf.getboolean('DEFAULT', 'use_gateway') \
            if 'use_gateway' in self.conf.defaults() else True
        self.__str__()
        add_methods_to_pandas_dataframe(self.logger)

    def __enter__(self, system=None):
        if self.use_gateway:
            self.init_tunnels(system)
        return self

    def __exit__(self, etype, *args):
        self.stop_server()
        return None

    def __str__(self):
        return ('alldays/nologs: {0}/{1}\ndata shape: {2}\nlogs (keys): {3}\n'
                'threaded: {4}\nserver is set up?: {5}\nusing gateway? {7}\n'
                'Settings file: {6}\n\n{8}'
                ''.format(self.alldays,
                          self.nologs,
                          self.data.shape,
                          self.logs.keys(),
                          not self.safe,
                          'Yes' if self.server else 'No',
                          self.settings_file,
                          self.use_gateway,
                          self.dump_config()
                          )
                )

    def dump_config(self):
        """ Returns a string with the configuration file contents """
        config = StringIO()
        self.conf.write(config)
        config.seek(0)
        return config.read()

    def plot(self, *args, **kwargs):
        """ Convenience method for calling gen_plot.plot_var """
        return gen_plot.plot_var(self.data,
                                 *args,
                                 logger=self.logger,
                                 **kwargs)

    def select(self, *args, **kwargs):
        """ Convenience method for calling df_tools.select_var """
        return df_tools.select_var(self.data,
                                   *args,
                                   logger=self.logger,
                                   **kwargs)

    def init_tunnels(self, system=None):
        """
        Description:
        Arguments:
            - system
                Type: string
                Default: None
                Description: system to initialize the tunnels. If nothing given
                             it initializes tunnels for all systems in
                             self.systems
        Returns:
            SSHTunnelForwarder instance (non-started)

            Calls sshtunnel and returns a ssh server with all tunnels
            established SSHTunnelForwarder instance is returned non-started
        """
        self.logger.info('Initializing tunnels')
        if not self.conf:
            self.conf = read_config(self.settings_file)

        jumpbox_addr = self.conf.get('GATEWAY', 'ip_or_hostname')
        jumpbox_port = self.conf.getint('GATEWAY', 'ssh_port')
        rbal = []
        lbal = []
        tunnelports = {}

        for _sys in [system] if system else self.systems:
            rbal.append((self.conf.get(_sys, 'ip_or_hostname'),
                         self.conf.getint(_sys, 'ssh_port')))
            lbal.append(('', self.conf.getint(_sys, 'tunnel_port') or
                         randint(61001, 65535)))  # if local port is 0, random
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
            self.start_server()
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

    def get_sftp_session(self, system):
        """ Open an sftp session to system
            By default the connection is done via SSH tunnels.
        """
        if system not in self.conf.sections():
            self.logger.error('%s | System not found in configuration',
                              system)
            raise SFTPSessionError('connection to %s failed' % system)

        if self.use_gateway:
            remote_system_address = '127.0.0.1'
            remote_system_port = self.server.tunnelports[system]
        else:
            remote_system_address = self.conf.get(system, 'ip_or_hostname')
            remote_system_port = self.conf.get(system, 'ssh_port')

        self.logger.info('%s | Connecting to %sport %s',
                         system,
                         'tunnel ' if self.use_gateway else '',
                         remote_system_port)

        ssh_pass = self.conf.get(system, 'password').strip("\"' ") or None \
            if self.conf.has_option(system, 'password') else None

        ssh_key = self.conf.get(system,
                                'identity_file').strip("\"' ") or None \
            if self.conf.has_option(system, 'identity_file') else None

        user = self.conf.get(system, 'username') or None \
            if self.conf.has_option(system, 'username') else None
        try:
            return SftpSession(hostname=remote_system_address,
                               ssh_user=user,
                               ssh_pass=ssh_pass,
                               ssh_key=ssh_key,
                               ssh_timeout=self.conf.get(system,
                                                         'ssh_timeout'),
                               ssh_port=remote_system_port,
                               logger=self.logger)
        except SFTPSessionError:
            raise SFTPSessionError('connection to %s failed' % system)

    def get_data_and_logs(self, system):
        """ Open an sftp session to system and collects the CSVs, generating a
            pandas dataframe as outcome
            By default the connection is done via SSH tunnels.
        """
        sftp_session = self.get_sftp_session(system)

        if not sftp_session:
            return (None, None)
        with sftp_session as session:
            data = self.get_system_data(session, system)
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

        destdir = self.conf.get(system, 'folder') or '.'
        # try:  # Test if remote folder is reachable
        #     with changedir(destdir, module=session):
        #     # session.chdir(destdir)
        #         self.logger.debug('%s | Changing to remote folder: %s',
        #                           system,
        #                           destdir)
        #     # session.chdir()  # revert back to home folder
        # except IOError:
        #     self.logger.error('%s | Directory "%s" not found at destination',
        #                       system,
        #                       self.conf.get(system, 'folder'))
        #     return data

        # filter remote files on extension and date
        # using MONTHS to avoid problems with locale rather than English
        # especially under Windows environments
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
            data.apply_calcs(calc_file, system)
            self.logger.info('%s | Dataframe shape after calculations: %s',
                             system, data.shape)
        return data

    def files_lookup(self,
                     hostname=None,
                     filespec_list=None,
                     compressed=False,
                     **kwargs):
        """
         Connects to a remote system via SFTP and looks for the filespec_list
        in the remote host. Also works locally.
         Files that will be returned must match every item in filespec_list,
        i.e. data*2015*csv would match data_2015.csv, 2015_data_full.csv but
        not data_2016.csv.
         When working with the local filesystem, filespec_list may contain
        absolute paths.

        Working with local filesystem if hostname is None

        Returns:
         - files: list of files matching the filespec_list in the remote
           host or a string with wildmarks (*), i.e. data*2015*.csv

        **kwargs (optional):
        - sftp_session: already established sftp session
        - files_folder: folder where files are located, either on sftp srv or
                        local filesystem
        - system: filter also on system name (case insensitive)
        """
        sftp_session = kwargs.get('sftp_session', None)
        files_folder = kwargs.get('files_folder', '.')
        system = kwargs.get('system', None)

        if files_folder[-1] == os.sep:
            files_folder = files_folder[:-1]  # remove trailing separator (/)

        if filespec_list and not isinstance(filespec_list, list):
            filespec_list = filespec_list.split('*')
        # default if no filter given is just the extension of the files
        spec_list = filespec_list[:] or ['.zip' if compressed else '.csv']
        if system:  # filter on system ID too
            spec_list.append(system)

        if sftp_session:
            self.logger.debug('Using established sftp session...')
            filesource = sftp_session
        else:
            self.logger.debug('Using local filesystem to get the files')
            filesource = os

        # get file list by filtering with taglist (case insensitive)
        try:
            with change_dir(directory=files_folder,
                            module=filesource):
                files = ['{}/{}'.format(filesource.getcwd(), f)
                         for f in filesource.listdir('.')
                         if all([val.upper() in f.upper()
                                 for val in spec_list])]
            if not files and not hostname:
                files = filespec_list  # For absolute paths
        except EnvironmentError:  # cannot do the chdir
            self.logger.error('%s | Directory "%s" not found at destination',
                              system,
                              files_folder)
            return
        return files

    def load_zipfile(self, zip_file, sftp_session=None, system=None):
        """
         Inflate a zip file and call get_stats_from_host with the decompressed
        CSV files
        """
        temp_dir = tempfile.gettempdir()
        self.logger.info('Decompressing ZIP file %s into %s...',
                         zip_file,
                         temp_dir)
        _df = pd.DataFrame()
        if not isinstance(sftp_session, SFTPClient):
            sftp_session = __builtin__  # open local file
        with sftp_session.open(zip_file) as file_descriptor:
            c = StringIO()
            c.write(file_descriptor.read())
            c.seek(0)
        decompressed_files = []
        try:
            with zipfile.ZipFile(c, 'r') as zip_data:
                # extract all to /tmp
                zip_data.extractall(temp_dir)
                # Recursive call to get_stats_from_host using localfs
                decompressed_files = [os.path.join(temp_dir,
                                                   f.filename)
                                      for f in zip_data.filelist]
                _df = self.get_stats_from_host(
                    filespec_list=decompressed_files
                )
        except (zipfile.BadZipfile, zipfile.LargeZipFile) as exc:
            self.logger.error('Bad ZIP file: %s', zip_file)
            self.logger.error(exc)
        finally:
            for a_file in decompressed_files:
                self.logger.debug('Deleting file %s', a_file)
                # os.remove(a_file)
            c.close()

        return _df

    def get_stats_from_host(self,
                            filespec_list=None,
                            hostname=None,
                            compressed=False,
                            sftp_session=None,
                            **kwargs):
        """
         Connects to a remote system via SFTP and reads the CSV files, which
        might be compressed in ZIP files, then call the csv-pandas conversion
        function.
         Working with local filesystem if hostname is None
         Returns: pandas dataframe

        **kwargs (optional):
        files_folder: folder where files are located, either on sftp server or
                      local filesystem
        """
        _df = pd.DataFrame()
        if hostname and not sftp_session:
            self.logger.error('Cannot gather remote data without a session')
            return _df
        files = self.files_lookup(hostname=hostname,
                                  filespec_list=filespec_list,
                                  compressed=compressed,
                                  sftp_session=sftp_session,
                                  **kwargs)
        if not files:
            self.logger.debug('Nothing gathered from %s, no files were '
                              'selected for pattern "%s"',
                              hostname or 'local system',
                              filespec_list)
            return _df
        for a_file in files:
            self.logger.info(a_file)
            if compressed:
                _df = _df.combine_first(
                    self.load_zipfile(zip_file=a_file,
                                      sftp_session=sftp_session)
                )
                system = kwargs.get('system')
                if system:
                    _df = df_tools.consolidate_data(_df,
                                                    dataframe=self.data,
                                                    system=system)

            else:
                _df = _df.combine_first(
                    df_tools.dataframize(data_file=a_file,
                                         sftp_session=sftp_session,
                                         logger=self.logger)
                )
        # if sftp_session:
        #     self.logger.debug('Closing sftp session')
        #     sftp_session.close()
        return _df

    def check_if_tunnel_is_up(self, system):
        """
        Return true if there's a tuple in self.server.tunnel_is_up such as:
            {('0.0.0.0', port): True}
        where port is the tunnel listen port for 'system'
        """
        port = self.server.tunnelports[system]
        return any(port in address_tuple for address_tuple
                   in self.server.tunnel_is_up.iterkeys()
                   if self.server.tunnel_is_up[address_tuple])

    def collect_system_data(self, system):
        """
        Collect everything for a given system
        """
        # Get data from the remote system
        try:
            self.logger.info('%s | Collecting statistics...', system)
            if not self.check_if_tunnel_is_up(system):
                self.logger.error('%s | System not reachable!', system)
                raise IOError
            (result_data, result_log) = self.get_data_and_logs(system)
            # self.logger.debug('%s | Putting results in queue', system)
        except (IOError, SFTPSessionError):
            result_data = pd.DataFrame()
            result_log = 'Could not get information from this system'
        # self.results_queue.put((system, data, log))
        self.logger.debug('%s | Consolidating results', system)
        self.data = df_tools.consolidate_data(result_data,
                                              dataframe=self.data,
                                              system=system)
        self.logs[system] = result_log
        self.results_queue.put(system)

    def main_threads(self):
        """ Threaded method for main() """
        with self:  # calls init_tunnels
            for system in self.systems:
                thread = threading.Thread(target=self.collect_system_data,
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
                if self.use_gateway:
                    self.init_tunnels(system)
                self.collect_system_data(system)
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
            self.logger.error('Unexpected error: %s', repr(exc))

    def to_pickle(self, name, compress=False):
        """ Save collector object to [optionally] gzipped pickle """
        buffer_object = StringIO()
        col_copy = copy.copy(self)
        # cannot pickle a Queue, logging or sshtunnel objects
        col_copy.results_queue = col_copy.logger = col_copy.server = None
        pickle.dump(obj=col_copy,
                    file=buffer_object,
                    protocol=pickle.HIGHEST_PROTOCOL)
        buffer_object.flush()
        if name.endswith('.gz'):
            compress = True
            name = name.rsplit('.gz')[0]  # we append the .gz extension below

        if compress:
            output = gzip
            name = "%s.gz" % name
        else:
            output = __builtin__

        with output.open(name, 'wb') as pkl_out:
            pkl_out.write(buffer_object.getvalue())
        buffer_object.close()


def read_pickle(name, compress=False, logger=None):
    """ Properly restore dataframe plus its metadata from pickle store """
    if compress or name.endswith('.gz'):
        mode = gzip
    else:
        mode = __builtin__

    with mode.open(name, 'rb') as picklein:
        collector = pickle.load(picklein)
    collector.logger = logger or init_logger()
    return collector


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


def add_methods_to_pandas_dataframe(logger=None):
    """ Add custom methods to pandas.DataFrame """
    pd.DataFrame.oper = calculations.oper
    pd.DataFrame.oper_wrapper = calculations.oper_wrapper
    pd.DataFrame.recursive_lis = calculations.recursive_lis
    pd.DataFrame.apply_calcs = calculations.apply_calcs
    pd.DataFrame.clean_calcs = calculations.clean_calcs
    pd.DataFrame.logger = logger or init_logger()
