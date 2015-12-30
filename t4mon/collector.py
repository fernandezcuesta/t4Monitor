#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
              start()
                 |
(serial_handler or threaded_handler)
                 v
          get_data_and_logs()
             /            \
            |              |
            v              v
   get_system_data()  get_system_logs()
            |
            v
 get_stats_from_host()

"""
from __future__ import absolute_import

import os
import copy
import gzip
import Queue
import zipfile
import datetime as dt
import tempfile
import threading
import __builtin__
import ConfigParser

from cStringIO import StringIO
from contextlib import contextmanager

import tqdm
import pandas as pd
import sshtunnel
from paramiko import SFTPClient, SSHException

from sshtunnels.sftpsession import SftpSession, SFTPSessionError

from . import df_tools, gen_plot, calculations
from .logger import init_logger

try:
    import cPickle as pickle
except ImportError:
    import pickle

__all__ = ('add_methods_to_pandas_dataframe',
           'Collector',
           'load_zipfile',
           'read_pickle',
           'read_config')

# CONSTANTS
DEFAULT_SETTINGS_FILE = os.path.join(os.getcwd(), 'settings.cfg')
if not os.path.exists(DEFAULT_SETTINGS_FILE):
    DEFAULT_SETTINGS_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'conf',
        'settings.cfg'
    )
# Avoid using locale in Linux+Windows environments, keep these lowercase
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


@contextmanager
def change_dir(directory, module):
    """
    Context manager for restoring the current working directory
    """
    module = module or os
    current_dir = module.getcwd()
    module.chdir(directory)
    yield
    module.chdir(current_dir)


def get_datetag(date=None):
    """
    Return date in '%d%b%Y' format, locale independent
    If no date is specified, current date is returned
    """
    if not date:
        date = dt.date.today()
    return '%02i%s%i' % (date.day,
                         MONTHS[date.month-1],
                         date.year)


class ConfigReadError(Exception):

    """
    Exception subclass (dummy) raised while reading configuration file
    """
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

    Class arguments:
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

          - All class arguments as defined above

          - conf
              Type: ConfigParser.SafeConfigParser
              Default: SafeConfigParser object as obtained from sample
                       configuration file
              Description: Object containing the settings as read from
                           settings_file (passed as argument)

          - data
              Type: pandas.DataFrame
              Default: pandas.DataFrame()
              Description: Multiple index dataframe containing the data
                           collected for all the systems. The indices are:
                           - Datetime: sample timestamp
                           - System: system ID for the current sample

          - filecache
              Type: dict
              Default: {}
              Description: (key, value) dictionary containting for each
                           remote folder for a system (key=(system, folder)),
                           the list of files (value) in the remote system
                           (or localfs if working locally) cached to avoid
                           doing sucessive file lookups (slow when number of
                           files is high)
          - logs
              Type: dict
              Default: {}
              Description: Output from running remotely the command specified
                           in the configuration file (MISC/remote_log_cmd)

          - results_queue
              Type: Queue.Queue()
              Default: empty Queue object
              Description: Queue containing the system IDs which data
                           collection is ready

          - server
              Type: SSHTunnel.SSHTunnelForwarder
              Default: None
              Description: Object representing the tunnel server

          - systems
              Type: list
              Default: []
              Description: List containing the system IDs as configured in the
                           settings file sections

          - use_gateway:
              Type: boolean
              Default: True
              Description: Whether or not the remote systems are behind an SSH
                           proxy. It defines if the connectivity is done via
                           tunnels or directly.

      Usage:

          with Collector(**options) as col:
              ...
                  operations
              ...

       or

           col = Collector(**options)
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
        self.conf = read_config(settings_file)
        self.data = pd.DataFrame()
        self.filecache = {}
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
        """
        Return a string with the configuration file contents
        """
        config = StringIO()
        self.conf.write(config)
        config.seek(0)
        return config.read()

    def plot(self, *args, **kwargs):  # pragma: no cover
        """
        Convenience method for calling gen_plot.plot_var
        """
        return gen_plot.plot_var(self.data,
                                 *args,
                                 logger=self.logger,
                                 **kwargs)

    def select(self, *args, **kwargs):  # pragma: no cover
        """
        Convenience method for calling df_tools.select_var
        """
        return df_tools.select_var(self.data,
                                   *args,
                                   logger=self.logger,
                                   **kwargs)

    def init_tunnels(self, system=None):
        """
        Initialize SSH tunnels using sshtunnel and paramiko libraries
        Arguments:
            - system
                Type: string
                Default: None
                Description: system to initialize the tunnels. If nothing given
                             it initializes tunnels for all systems in
                             self.systems
        Return:
            SSHTunnelForwarder instance (non-started) with all tunnels already
            established

        """
        self.logger.info('Initializing tunnels')
        if not self.conf:
            self.conf = read_config(self.settings_file)

        jumpbox_addr = self.conf.get('GATEWAY', 'ip_or_hostname')
        jumpbox_port = self.conf.getint('GATEWAY', 'ssh_port')
        rbal = []
        lbal = []
        tunnelports = {}
        systems = [system] if system else self.systems

        for _sys in systems:
            rbal.append((self.conf.get(_sys, 'ip_or_hostname'),
                         self.conf.getint(_sys, 'ssh_port')))
            lbal.append(('', self.conf.getint(_sys, 'tunnel_port')))
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
                threaded=True,
                logger=self.logger,
                ssh_private_key=pkey,
                set_keepalive=15
            )
            self.server.check_local_side_of_tunnels()
            self.start_server()
            # Add the system<>port bindings to the return object
            self.server.tunnelports = dict(zip(systems,
                                               self.server.local_bind_ports))
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

    def start_server(self):  # pragma: no cover
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

    def stop_server(self):  # pragma: no cover
        """
        Dummy function to stop SSH servers
        """
        try:
            if self.server and self.server._is_started:
                self.logger.info('Closing connection to gateway')
                self.server.stop()
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

    def check_if_tunnel_is_up(self, system):
        """
        Return true if there's a tuple in self.server.tunnel_is_up such as:
            {('0.0.0.0', port): True}
        where port is the tunnel listen port for 'system'
        """
        if not self.server or system not in self.server.tunnelports:
            return False
        port = self.server.tunnelports[system]
        return any(port in address_tuple for address_tuple
                   in self.server.tunnel_is_up.iterkeys()
                   if self.server.tunnel_is_up[address_tuple])

    def get_sftp_session(self, system):
        """
        Open an sftp session to system
        By default the connection is done via SSH tunnels (controlled by
        self.use_gateway)

        Return an SFTPClient object
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
            remote_system_port = self.conf.getint(system, 'ssh_port')

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

    def files_lookup(self,
                     hostname=None,
                     filespec_list=None,
                     compressed=False,
                     **kwargs):
        """
        Connect to a remote system via SFTP and looks for the filespec_list
        in the remote host. Also works locally.
        Files that will be returned must match every item in filespec_list,
        i.e. data*2015*csv would match data_2015.csv, 2015_data_full.csv but
        not data_2016.csv.
        When working with the local filesystem, filespec_list may contain
        absolute paths.

        Working with local filesystem if not a valid sftp_session is passed

        Return:
         - files: list of files matching the filespec_list in the remote
           host or a string with wildmarks (*), i.e. data*2015*.csv

        **kwargs (optional):
        - sftp_session: already established sftp session
        - files_folder: folder where files are located, either on sftp srv or
                        local filesystem
        """
        sftp_session = kwargs.get('sftp_session', None)
        files_folder = kwargs.get('files_folder', '.')

        if files_folder[-1] == os.sep:
            files_folder = files_folder[:-1]  # remove trailing separator (/)

        if filespec_list and not isinstance(filespec_list, list):
            filespec_list = filespec_list.split('*')
        # default if no filter given is just the extension of the files
        spec_list = filespec_list[:] or ['.zip' if compressed else '.csv']

        if sftp_session:
            self.logger.debug('Using established sftp session...')
            self.logger.debug("Looking for remote files (%s) at '%s'",
                              spec_list,
                              files_folder)
            filesource = sftp_session
        else:
            self.logger.debug('Using local filesystem to get the files')
            self.logger.debug("Looking for local files (%s) at '%s'",
                              spec_list,
                              os.path.abspath(files_folder))
            filesource = os
        # get file list by filtering with taglist (case insensitive)
        try:
            with change_dir(directory=files_folder,
                            module=filesource):
                key = (hostname or 'localfs', files_folder)
                if key not in self.filecache:  # fill the cache
                    self.filecache[key] = filesource.listdir('.')
                else:
                    self.logger.debug('Using cached file list for %s', key)
                files = ['{}/{}'.format(filesource.getcwd(), f)
                         for f in self.filecache[key]
                         if all([v.upper() in f.upper() for v in spec_list])
                         ]
            if not files and not sftp_session:
                files = filespec_list  # Relative and absolute paths (local)
        except EnvironmentError:  # cannot do the chdir
            self.logger.error('%s | Directory "%s" not found at destination',
                              hostname,
                              files_folder)
            return
        return files

    def get_stats_from_host(self,
                            filespec_list=None,
                            hostname=None,
                            compressed=False,
                            sftp_session=None,
                            **kwargs):
        """
        Connect to a remote system via SFTP and reads the CSV files, which
        might be compressed in ZIP files, then call the csv-pandas conversion
        function.
        Working with local filesystem if hostname is None

        Return: pandas dataframe

        **kwargs (optional):
        files_folder: folder where files are located, either on sftp server or
                      local filesystem
        """
        _df = pd.DataFrame()
        _dz = pd.DataFrame()

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

        progressbar_prefix = 'Loading {}files{}'.format(
                                  'compressed ' if compressed else '',
                                  ' from %s' % hostname if hostname else ''
                              )
        for a_file in tqdm.tqdm(files,
                                leave=True,
                                desc=progressbar_prefix,
                                disable=compressed,
                                unit='Archives' if compressed else 'Files'):
            if compressed:
                _dz = _dz.combine_first(
                    self._load_zipfile(zip_file=a_file,
                                       sftp_session=sftp_session)
                )
                if hostname:
                    _df = df_tools.consolidate_data(_dz,
                                                    dataframe=_df,
                                                    system=hostname)
                else:
                    return _dz

            else:
                _df = _df.combine_first(
                    df_tools.dataframize(data_file=a_file,
                                         sftp_session=sftp_session,
                                         logger=self.logger)
                )
        return _df

    def get_system_logs(self, ssh_session, system, log_cmd=None):
        """
        Get log info from the remote system, assumes an already established
        ssh tunnel.
        """
        if not log_cmd:
            self.logger.error('No command was specified for log collection')
            return
        self.logger.warning('Getting log output from %s (Remote command: %s), '
                            'may take a while...',
                            system,
                            log_cmd)
        try:  # ignoring stdin and stderr for OpenVMS SSH2
            (_, stdout, _) = ssh_session.\
                             exec_command(log_cmd)
            return stdout.readlines()
        except Exception as _exc:
            self.logger.error('%s | Error occurred while getting logs: %s',
                              system, repr(_exc))
            return None

    def get_single_day_data(self, given_date=None):
        """
        Given a single date, collect all systems data for such date

        Arguments:

        - given_date
            Type: datetime
            Default: today's datetime
            Description: define for which day the data will be collected from
                         the remote systems
        """

        def _single_day_and_system_data(system, given_date=None):
            given_date = get_datetag(given_date)
            self.logger.info('Collecting data for system: %s; day: %s',
                             system,
                             given_date)
            with self.get_sftp_session(system) as session:
                result_data = self.get_system_data(session,
                                                   system,
                                                   given_date)
                self.data = df_tools.consolidate_data(result_data,
                                                      dataframe=self.data,
                                                      system=system)
                self.results_queue.put(system)  # flag this system as done
        with self:  # open tunnels
            self.run_systemwide(_single_day_and_system_data,
                                given_date)

    def get_system_data(self, session, system, day=None):
        """
        Create pandas DF from current session CSV files downloaded via SFTP

        Arguments:
        - session
          Type: SftpClient session (already initialized)
          Description: sftp session agains the remote system

        - system
          Type: str
          Description: remote system hostname, as present in settings file

        - day
          Type: string
          Default: datetime.date.today() in the format '%d%b%Y'
          Description: String identifying for which day the data will be
                       collected
        """
        data = pd.DataFrame()
        destdir = self.conf.get(system, 'folder') or '.'

        # Filter only on '.csv' extension if alldays
        tag_list = ['.csv'] + ([] if self.alldays and not day
                               else [day or get_datetag()])

        try:  # if present, also filter on cluster id
            tag_list.append(self.conf.get(system, 'cluster_id').lower())
        except Exception:
            pass

        data = self.get_stats_from_host(hostname=system,
                                        filespec_list=tag_list,
                                        sftp_session=session,
                                        files_folder=destdir)
        if data.empty:
            self.logger.warning('%s | No data was obtained!', system)
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

    def get_data_and_logs(self, system):
        """
        Collect everything needed for a system.
        Open an sftp session to system and collects the CSVs, generating a
        pandas dataframe as outcome.
        By default the connection is done via SSH tunnels.
        """
        # TODO: allow parallel (data | log) collection
        try:
            self.logger.info('%s | Collecting statistics...', system)
            if self.use_gateway and not self.check_if_tunnel_is_up(system):
                self.logger.error('%s | System not reachable!', system)
                raise SFTPSessionError

            # Get an sftp session
            sftp_session = self.get_sftp_session(system)
            if not sftp_session:
                raise SFTPSessionError('Cannot open an SFTP session to %s' %
                                       system)
            with sftp_session as session:  # open the session
                # Get data from the remote system
                result_data = self.get_system_data(session, system)
                # Done gathering data, now get the logs
                if self.nologs or result_data.empty \
                   or not self.conf.has_option('MISC', 'remote_log_cmd'):
                    result_logs = '{0} | Log collection omitted'.format(system)
                    self.logger.info(result_logs)
                else:
                    result_logs = self.get_system_logs(
                        sftp_session.ssh_transport,
                        system,
                        self.conf.get('MISC', 'remote_log_cmd')
                    ) or '{} | Missing logs!'.format(system)
        except (IOError, SFTPSessionError):
            result_data = pd.DataFrame()
            result_logs = 'Could not get information from this system'

        self.logger.debug('%s | Consolidating results', system)
        self.data = df_tools.consolidate_data(result_data,
                                              dataframe=self.data,
                                              system=system)
        self.logs[system] = result_logs
        self.results_queue.put(system)

    def run_systemwide(self, target, *args):
        """
        Run a target function systemwide and wait until all of them are
        finished.
        The target function is supposed to leave the value for 'system' in
        self.results_queue.
        """
        for system in self.systems:
            thread = threading.Thread(target=target,
                                      name=system,
                                      args=tuple([system] + list(args)))
            thread.daemon = True
            thread.start()
        # wait for threads to end, first one to finish will leave
        # the result in the queue
        for system in self.systems:
            self.logger.info('%s | Done collecting data!',
                             self.results_queue.get())

    def threaded_handler(self):
        """
        Initialize tunnels and collect data&logs, threaded mode
        """
        with self:  # calls init_tunnels
            self.run_systemwide(self.get_data_and_logs)

    def serial_handler(self):
        """
        Get data&logs. Serial (legacy) handler, working inside a for loop
        """
        for system in self.systems:
            self.logger.info('%s | Initializing tunnel', system)
            try:
                if self.use_gateway:
                    self.init_tunnels(system=system)
                self.get_data_and_logs(system=system)
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
                self.serial_handler()
            else:
                self.threaded_handler()
        except (sshtunnel.BaseSSHTunnelForwarderError, AttributeError) as exc:
            self.logger.error('Could not initialize the SSH tunnels, '
                              'aborting (%s)', repr(exc))
        except SSHException:
            self.logger.error('Could not open remote connection')
        except Exception as exc:
            self.logger.exception(exc)

    def to_pickle(self, name, compress=False):
        """
        Save collector object to [optionally] gzipped pickle
        """
        buffer_object = StringIO()
        col_copy = copy.copy(self)
        # cannot pickle a Queue, logging, or sshtunnel objects
        col_copy.results_queue = col_copy.logger = col_copy.server = None
        pickle.dump(obj=col_copy,
                    file=buffer_object,
                    protocol=pickle.HIGHEST_PROTOCOL)
        buffer_object.flush()
        if name.endswith('.gz'):
            compress = True
            name = name.rsplit('.gz')[0]  # will append the .gz extension below

        if compress:
            output = gzip
            name = "%s.gz" % name
        else:
            output = __builtin__

        with output.open(name, 'wb') as pkl_out:
            pkl_out.write(buffer_object.getvalue())
        buffer_object.close()

    def _load_zipfile(self, zip_file, sftp_session=None):
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
                # extract all to a temporary folder
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
            self.logger.exception(exc)
        finally:
            for a_file in decompressed_files:
                self.logger.debug('Deleting file %s', a_file)
                os.remove(a_file)
            c.close()

        return _df


def load_zipfile(zipfile, system=None):
    """
    Convenience method for Collector._load_zipfile()
    """
    col = Collector(alldays=True, nologs=True)
    return col.get_stats_from_host(zipfile, hostname=system, compressed=True)


def read_pickle(name, compress=False, logger=None):
    """
    Restore dataframe plus its metadata from (optionally deflated) pickle store
    """
    if compress or name.endswith('.gz'):
        mode = gzip
    else:
        mode = __builtin__

    with mode.open(name, 'rb') as picklein:
        collector = pickle.load(picklein)
    collector.logger = logger or init_logger()
    return collector


def read_config(settings_file=None, **kwargs):
    """
    Return ConfigParser object from configuration file
    """
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
    """
    Add custom methods to pandas.DataFrame
    """
    pd.DataFrame.oper = calculations.oper
    pd.DataFrame.oper_wrapper = calculations.oper_wrapper
    pd.DataFrame.recursive_lis = calculations.recursive_lis
    pd.DataFrame.apply_calcs = calculations.apply_calcs
    pd.DataFrame.clean_calcs = calculations.clean_calcs
    pd.DataFrame.logger = logger or init_logger()
