#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
::

                  start()
                     |
                     v
              get_data_and_logs()
                 /            \\
                |              |
                v              v
       get_system_data()  get_system_logs()
                |
                v
     get_stats_from_host()

"""
from __future__ import absolute_import

import os
import re
import gzip
import zipfile
import datetime as dt
import tempfile
import threading
from contextlib import contextmanager

import six
import tqdm
import pandas as pd
import sshtunnel
from paramiko import SFTPClient, SSHException
from six.moves import queue, cPickle, builtins, cStringIO
from sshtunnels.sftpsession import SftpSession, SFTPSessionError

from . import df_tools, gen_plot, arguments, calculations
from .logger import init_logger

__all__ = ('add_methods_to_pandas_dataframe',
           'Collector',
           'load_zipfile',
           'read_pickle')

# Avoid using locale in Linux+Windows environments, keep these lowercase
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


# sshtunnel.DAEMON = True  # Cleanly stop threads when quitting


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
    return '{0:02d}{1}{2}'.format(date.day,
                                  MONTHS[date.month - 1],
                                  date.year)


class Collector(object):

    """
    Data collection class.

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

        - **threaded**: default mode.
          Most operations (data collection, reporting) are executed in parallel
          for each system
        - **safe**: serial mode, slower.
          All operations are executed serially system by system.

    Arguments:
        alldays (boolean or False):
            Define whether or not filter remote files on current date.
            If ``True``, remote files will be filtered on a timestamp with the
            ``DDMMMYYY`` format (i.e. ``20may2015``).
        logger (Optional[logging.Logger]):
            Logger object passed from an external function. A new logger is
            created by calling :func:`t4mon.logger.init_logger` if nothing is
            passed.
        loglevel (str):
            logger level, if no ``logger`` passed
        nologs (boolean or False)
            Skip remote log collection. An indication message will be shown in
            the report showing that the log collection was omitted.
        safe (boolean or False):
            Define the mode (safe or threaded) for most of the class methods.
        settings_file (str or :const:`t4mon.arguments.DEFAULT_SETTINGS_FILE`)
            Define the name of the configuration file.


    Attributes:

        alldays (boolean or False):
            Define whether or not filter remote files on current date.
            If ``True``, remote files will be filtered on a timestamp with the
            ``DDMMMYYY`` format (i.e. ``20may2015``).

        conf (configParser.ConfigParser):
            Object containing the settings as read from settings_file (passed
            as argument).
            Default: ``ConfigParser`` object as obtained from sample
            configuration file.

        data (pandas.DataFrame):
            Multiple-index dataframe containing the data collected for all the
            systems. The indices are:

            - ``Datetime``: sample timestamp
            - ``system``: system ID for the current sample

            Default: ``pandas.DataFrame()``

        filecache (dict):
            (key, value) dictionary containting for each remote folder for a
            system (key=(system, folder)), the list of files (value) in the
            remote system (or localfs if working locally) cached to avoid
            doing sucessive file lookups (slow when number of files is high).
            Default: empty dict

        logger (Optional[logging.Logger]):
            Logger object passed from an external function. A new logger is
            created by calling :func:`t4mon.logger.init_logger` if nothing is
            passed.

        logs (dict):
            Output from running remotely the command specified in the
            configuration file (``MISC/remote_log_cmd``).
            Default: empty dict

        nologs (boolean)
            Skip remote log collection. An indication message will be shown in
            the report showing that the log collection was omitted.
            Default: ``False``

        results_queue (queue.Queue)
            Queue containing the system IDs which data collection is ready.
            Default: empty ``Queue`` object

        safe (boolean):
            Define the mode (safe vs threaded) for most of this class methods.
            Default: ``False``

        server (SSHTunnel.SSHTunnelForwarder):
            Object representing the tunnel server.
            Default: ``None``

        settings_file(str):
            Name of the file containing the settings
            Default: :const:`t4mon.arguments.DEFAULT_SETTINGS_FILE`

        systems (list):
            List containing the system IDs as configured in the settings file
            sections.
            Default: empty list

      Examples:

        >>> with Collector(**options) as col:
                # operations


        >>> col = Collector(**options)
        >>> col.init_tunnels()
        >>> # operations
        >>> col.stop_server()
    """

    def __init__(self,
                 alldays=False,
                 logger=None,
                 loglevel=None,
                 nologs=False,
                 safe=False,
                 settings_file=None,
                 **kwargs):

        self.alldays = alldays
        self.conf = arguments.read_config(settings_file)
        self.data = pd.DataFrame()
        self.filecache = {}
        self.logger = logger or init_logger(loglevel)
        self.logs = {}
        self.nologs = nologs
        self.results_queue = queue.Queue()
        self.safe = safe
        self.settings_file = settings_file or arguments.DEFAULT_SETTINGS_FILE
        self.server = None
        self.systems = [item for item in self.conf.sections()
                        if item not in ['GATEWAY', 'MISC']]
        add_methods_to_pandas_dataframe(self.logger)

    def __enter__(self, system=None):
        self.init_tunnels(system)
        return self

    def __exit__(self, etype, *args):
        self.stop_server()
        return None

    def __str__(self):
        return ('alldays/nologs: {0}/{1}\ndata shape: {2}\nlogs (keys): {3}\n'
                'threaded: {4}\nserver is set up?: {5}\n'
                'Settings file: {6}\n\n{7}'
                ''.format(self.alldays,
                          self.nologs,
                          self.data.shape,
                          list(self.logs.keys()),
                          not self.safe,
                          'Yes' if self.server else 'No',
                          self.settings_file,
                          self.dump_config()
                          )
                )

    def _check_if_using_gateway(self, system=None):
        """ Check if the connection is tunneled over an SSH gateway or not """
        try:
            return self.conf.getboolean(system or 'DEFAULT', 'use_gateway')
        except six.moves.configparser.Error:
            return True

    def __getstate__(self):
        """ Method enabling class pickle """
        odict = self.__dict__.copy()
        if self.logger:
            odict['loggername'] = self.logger.name
        for item in ['logger', 'results_queue', 'server']:
            del odict[item]
        return odict

    def __setstate__(self, state):
        """ Method enabling class pickle """
        state['logger'] = init_logger(name=state.get('loggername'))
        if 'loggername' in state:
            del state['loggername']
        state['results_queue'] = queue.Queue()
        state['server'] = None
        self.__dict__.update(state)

    def dump_config(self):
        """
        Return a string with the configuration file contents
        """
        config = cStringIO()
        self.conf.write(config)
        config.seek(0)
        return config.read()

    def plot(self, *args, **kwargs):  # pragma: no cover
        """
        Convenience method for calling :meth:`.gen_plot.plot_var`
        """
        return gen_plot.plot_var(self.data,
                                 *args,
                                 logger=self.logger,
                                 **kwargs)

    def select(self, *args, **kwargs):  # pragma: no cover
        """
        Convenience method for calling :meth:`.df_tools.select`
        """
        return df_tools.select(self.data,
                               *args,
                               logger=self.logger,
                               **kwargs)

    def init_tunnels(self, system=None):
        """
        Initialize SSH tunnels using ``sshtunnel`` and ``paramiko`` libraries.

        Arguments:
        - system

            Type: string

            Default: ``None``

            Description:
            system to initialize the tunnels. If nothing given it initializes
            tunnels for all systems in ``self.systems``.

        Return:

            ``SSHTunnelForwarder`` instance (non-started) with all tunnels
            already established
        """
        if not self._check_if_using_gateway(system):
            return
        self.logger.info('Initializing tunnels')
        if not self.conf:
            self.conf = arguments.read_config(self.settings_file)

        jumpbox_addr = self.conf.get('GATEWAY', 'ip_or_hostname')
        jumpbox_port = self.conf.getint('GATEWAY', 'ssh_port')
        rbal = []
        lbal = []
        tunnelports = {}
        systems = [system] if system else self.systems
        sshtunnel.SSH_TIMEOUT = arguments.DEFAULT_SSH_TIMEOUT

        for _sys in systems:
            rbal.append((self.conf.get(_sys, 'ip_or_hostname'),
                         self.conf.getint(_sys, 'ssh_port')))
            lbal.append(('', self.conf.getint(_sys, 'tunnel_port')))
        if len(tunnelports) != len(set(tunnelports.values())):
            self.logger.error('Local tunnel ports MUST be different: {0}'
                              .format(tunnelports))
            raise sshtunnel.BaseSSHTunnelForwarderError
        try:
            pwd = self.conf.get('GATEWAY', 'password').strip("\"' ") or None \
                if self.conf.has_option('GATEWAY', 'password') else None
            pkey = self.conf.get('GATEWAY', 'identity_file').strip("\"' ") \
                or None if self.conf.has_option('GATEWAY', 'identity_file') \
                else None
            user = self.conf.get('GATEWAY', 'username') or None \
                if self.conf.has_option('GATEWAY', 'username') else None
            self.server = sshtunnel.open_tunnel(
                ssh_address_or_host=(jumpbox_addr, jumpbox_port),
                ssh_username=user,
                ssh_password=pwd,
                remote_bind_addresses=rbal,
                local_bind_addresses=lbal,
                threaded=True,
                logger=self.logger,
                ssh_pkey=pkey,
                ssh_private_key_password=pwd,
                set_keepalive=15.0,
                allow_agent=False,
                mute_exceptions=True,
                skip_tunnel_checkup=False,
            )
            self.server.is_use_local_check_up = True  # Check local side
            self._start_server()
            assert self.server.is_alive
            # Add the system<>port bindings to the return object
            self.server.tunnelports = dict(
                list(zip(systems, self.server.local_bind_ports))
            )
            self.logger.debug('Registered tunnels: {0}'
                              .format(self.server.tunnelports))
        except (sshtunnel.BaseSSHTunnelForwarderError, AssertionError):
            self.logger.error('{0}Could not open connection to remote server: '
                              '{1}:{2}'.format(
                                  '{0} | '.format(system) if system else '',
                                  jumpbox_addr,
                                  jumpbox_port
                              ))
            raise sshtunnel.BaseSSHTunnelForwarderError

    def _start_server(self):  # pragma: no cover
        """
        Start the SSH tunnels
        """
        if not self.server:
            raise sshtunnel.BaseSSHTunnelForwarderError
        try:
            self.logger.info('Opening connection to gateway')
            self.server.start()
            if not self.server.is_alive:
                raise sshtunnel.BaseSSHTunnelForwarderError(
                    "Couldn't start server"
                )
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

    def stop_server(self):  # pragma: no cover
        """
        Stop the SSH tunnels
        """
        try:
            if self.server and self.server.is_alive:
                self.logger.info('Closing connection to gateway')
                self.server.stop()
        except AttributeError as msg:
            raise sshtunnel.BaseSSHTunnelForwarderError(msg)

    def check_if_tunnel_is_up(self, system):
        """
        Return true if there's a tuple in :attr:`.server`'s' ``.tunnel_is_up``
        such as: ``{('0.0.0.0', port): True}``
        where port is the tunnel listen port for ``system``.

        Arguments:
            system (str): system which tunnel port will be checked
        Return:
            boolean
        """
        if not self.server or system not in self.server.tunnelports:
            return False
        port = self.server.tunnelports[system]
        return any(port in address_tuple for address_tuple
                   in six.iterkeys(self.server.tunnel_is_up)
                   if self.server.tunnel_is_up[address_tuple])

    def get_sftp_session(self, system):
        """
        By default the connection is done via SSH tunnels (controlled by
        configutation item `use_gateway`)

        Arguments:
            system (str): system where to open the SFTP session
        Return:
            ``paramiko.SFTPClient``
        """
        if system not in self.conf.sections():
            self.logger.error('{0} | System not found in configuration'
                              .format(system))
            raise SFTPSessionError('connection to {0} failed'.format(system))

        use_gateway = self._check_if_using_gateway(system)
        if use_gateway:
            remote_system_address = '127.0.0.1'
            remote_system_port = self.server.tunnelports[system]
        else:
            remote_system_address = self.conf.get(system, 'ip_or_hostname')
            remote_system_port = self.conf.getint(system, 'ssh_port')

        self.logger.info('{0} | Connecting to {1}port {2}'
                         .format(system,
                                 'tunnel ' if use_gateway else '',
                                 remote_system_port))

        ssh_pass = self.conf.get(system, 'password').strip("\"' ") or None \
            if self.conf.has_option(system, 'password') else None

        ssh_key = self.conf.get(system,
                                'identity_file').strip("\"' ") or None \
            if self.conf.has_option(system, 'identity_file') else None

        user = self.conf.get(system, 'username') or None \
            if self.conf.has_option(system, 'username') else None
        try:
            if six.PY3:
                ssh_timeout = self.conf.get(
                    system,
                    'ssh_timeout',
                    fallback=arguments.DEFAULT_SSH_TIMEOUT
                )
            else:
                ssh_timeout = self.conf.get(
                    system,
                    'ssh_timeout'
                ) if self.conf.has_option(
                    system,
                    'ssh_timeout'
                ) else arguments.DEFAULT_SSH_TIMEOUT

            return SftpSession(hostname=remote_system_address,
                               ssh_user=user,
                               ssh_pass=ssh_pass,
                               ssh_key=ssh_key,
                               ssh_timeout=ssh_timeout,
                               ssh_port=remote_system_port,
                               logger=self.logger)
        except SFTPSessionError:
            raise SFTPSessionError('connection to {0} failed'.format(system))

    def files_lookup(self,
                     hostname=None,
                     filespec_list=None,
                     compressed=False,
                     **kwargs):
        """
        Connect to a remote system via SFTP and get a list of files matching
        ``filespec_list`` in the remote host.
        Works locally when ``hostname=None``.

        Files that will be returned must match every item in ``filespec_list``,
        i.e. ``data*2015*csv`` would match ``data_2015.csv``,
        ``2015_data_full.csv`` but not ``data_2016.csv``.
        When working with the local filesystem, ``filespec_list`` may contain
        absolute paths.

        Working with local filesystem if not a valid sftp_session is passed

        Arguments:
            hostname (Optional[str]):
                Remote hostname where to download the CSV files.
                Default: working with local filesystem
            filespec_list (Optional[list]):
                list of files to look for (valid filespecs may contain
                wildcards (``*``))
            compressed (Optional[boolean]):
                whether to look for compressed or plain (CSV) files
                Default: ``False`` (look for CSV files)
        Keyword Arguments:
            sftp_session (paramiko.SFTPClient):
                already established sftp session
            files_folder: folder where files are located, either on sftp srv or
          local filesystem

        Return:
           list:
               files matching the filespec_list in the remote
               host or a string with wildmarks (*), i.e. ``data*2015*.csv``
        """
        sftp_session = kwargs.get('sftp_session', None)
        files_folder = kwargs.get('files_folder', '.')

        if files_folder[-1] == os.sep:
            files_folder = files_folder[:-1]  # remove trailing separator (/)

        if filespec_list and isinstance(filespec_list, six.string_types):
            filespec_list = filespec_list.split('*')
        # default if no filter given is just the extension of the files
        spec_list = filespec_list[:] or ['.zip' if compressed else '.csv']

        if sftp_session:
            self.logger.debug('Using established sftp session...')
            self.logger.debug("Looking for remote files ({0}) at '{1}'"
                              .format(spec_list, files_folder))
            filesource = sftp_session
        else:
            self.logger.debug('Using local filesystem to get the files')
            self.logger.debug("Looking for local files ({0}) at '{1}'"
                              .format(spec_list,
                                      os.path.abspath(files_folder)))
            filesource = os
        # get file list by filtering with taglist (case insensitive)
        try:
            with change_dir(directory=files_folder,
                            module=filesource):
                key = (hostname or 'localfs', files_folder)
                if key not in self.filecache:  # fill the cache
                    self.filecache[key] = filesource.listdir('.')
                else:
                    self.logger.debug('Using cached file list for {0}'
                                      .format(key))
                files = ['{0}/{1}'.format(filesource.getcwd(), f)
                         for f in self.filecache[key]
                         if all([v.upper() in f.upper() for v in spec_list])
                         ]
            if not files and not sftp_session:
                files = filespec_list  # Relative and absolute paths (local)
        except EnvironmentError:  # cannot do the chdir
            self.logger.error('{0} | Directory "{1}" not found at destination'
                              .format(hostname, files_folder))
            return
        return files

    def get_stats_from_host(self,
                            filespec_list=None,
                            hostname=None,
                            compressed=False,
                            sftp_session=None,
                            **kwargs):
        """
        Optionally connect to a remote system via SFTP to read CSV files, which
        might be compressed in ZIP files, then call the CSV-pandas conversion
        function.

        Arguments:
            filespec_list (Optional[list]):
                List of strings, each representing a valid file specification
                (wildcards (``*``) allowed
            hostname (Optional[str]):
                Remote hostname where to download the CSV files.
                Default: working with local filesystem
            compressed (Optional[boolean]):
                Whether or not the files matching ``filespec_list`` are
                compressed (deflate)
                Default: ``False`` (not compressed)
            sftp_session (Optional[paramiko.SFTPClient]):
                SFTP session to the remote ``hostname``
                Default: ``None`` (work with local filesystem)
            files_folder (Optional[str]):
                folder where files are located, either on sftp server or local
                filesystem
        Return:
            ``pandas.DataFrame``
        """
        _df = pd.DataFrame()

        files = self.files_lookup(hostname=hostname,
                                  filespec_list=filespec_list,
                                  compressed=compressed,
                                  sftp_session=sftp_session,
                                  **kwargs)
        if not files:
            self.logger.debug('Nothing gathered from {0}, no files were '
                              'selected for pattern "{1}"'
                              .format(hostname or 'local system',
                                      filespec_list))
            return _df

        progressbar_prefix = 'Loading {0}files{1}'.format(
            'compressed ' if compressed else '',
            ' from {0}'.format(hostname) if hostname else ''
        )
        for a_file in tqdm.tqdm(files,
                                leave=True,
                                desc=progressbar_prefix,
                                disable=compressed,
                                unit='Archives' if compressed else 'Files'):
            if compressed:
                _df = _df.combine_first(
                    self._load_zipfile(zip_file=a_file,
                                       sftp_session=sftp_session)
                )
                # if no hostname, try to infer it from the file name
                regex = 't4_(\w+)[0-9]_\w+_[0-9]{{4}}_[0-9]{{4}}_\w+.{0}'.\
                    format(os.path.splitext(a_file)[-1])
                if not hostname and re.search(regex, a_file):
                    hostname = re.search(regex, a_file).groups()[0]

                if hostname:
                    _df = df_tools.consolidate_data(_df,
                                                    system=hostname)

            else:
                _df = _df.combine_first(
                    df_tools.dataframize(data_file=a_file,
                                         session=sftp_session,
                                         logger=self.logger)
                )
        return _df

    def get_system_logs(self, ssh_session, system, command=None):
        """
        Get log info from the remote system, assumes an already established
        ssh tunnel.

        Arguments:
            ssh_session (paramiko.SSHClient):
                Active SSH client to the remote host where the ``command``
                will be invoked

            system (str):
                System representation as configured in
                :attr:`.settings_file` hostname where to download the CSV
                files. Working with local filesystem if ``None``

            command (Optional[str]):
                Command that will be executed in the remote host
        Return:
            str: stdout text representation (stdin and stderr ignored for
            OpenVMS' SSH2)
        """
        if not command:
            self.logger.error('No command was specified for log collection')
            return
        self.logger.warning('Getting log output from {0} (Remote command: {1})'
                            ', may take a while...'.format(system, command))
        try:  # ignoring stdin and stderr for OpenVMS SSH2
            (_, stdout, _) = ssh_session.exec_command(command)
            return stdout.readlines()
        except Exception as _exc:
            self.logger.error('{0} | Error occurred while getting logs: {1}'
                              .format(system, repr(_exc)))
            return None

    def get_single_day_data(self, given_date=None):
        """
        Given a single date, collect all systems data for such date and put the
        results in :attr:`.data`.

        Arguments:
            given_date (datetime):
                define for which day the data will be collected from the remote
                systems; default: today's datetime.
        """

        def _single_day_and_system_data(system, given_date=None):
            given_date = get_datetag(given_date)
            self.logger.info('Collecting data for system: {0}; day: {1}'
                             .format(system, given_date))
            with self.get_sftp_session(system) as session:
                result_data = self.get_system_data(session,
                                                   system,
                                                   given_date)
                self.data = df_tools.consolidate_data(result_data,
                                                      dataframe=self.data,
                                                      system=system)
                self.results_queue.put(system)  # flag this system as done

        with self:  # open tunnels
            self._run_systemwide(_single_day_and_system_data,
                                 given_date)

    def get_system_data(self, session, system, day=None):
        """
        Create pandas DF from current session CSV files downloaded via SFTP.

        Arguments:
            session (SftpClient):
                **Already initialized** sftp session to the remote system
            system (str):
                remote system hostname, as present in settings file
            day (str):
              String identifying for which day the data will be collected.
              Default: ``datetime.date.today()`` in the format ``%d%b%Y``
        Return:
            ``pandas.DataFrame``
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
            self.logger.warning('{0} | No data was obtained!'.format(system))
        else:
            self.logger.info('{0} | Dataframe shape obtained: {1}. '
                             'Now applying calculations...'.format(system,
                                                                   data.shape))
            calc_file = self.conf.get('MISC', 'calculations_file')
            if not os.path.isabs(calc_file):
                calc_file = '{0}{1}{2}'.format(
                    os.path.dirname(os.path.abspath(self.settings_file)),
                    os.sep,
                    calc_file
                )
            data.apply_calcs(calc_file, system)
            self.logger.info('{0} | Dataframe shape after calculations: {1}'
                             .format(system, data.shape))
        return data

    def get_data_and_logs(self, system):
        """
        Collect everything needed for a system.
        By default the connection is done via SSH tunnels.

        Arguments:
            system (str): Open an SFTP session to system and collect the CSVs
        Return:
            ``pandas.DataFrame``

        """
        # TODO: allow parallel (data | log) collection
        try:
            self.logger.info('{0} | Collecting statistics...'.format(system))
            if (
                self._check_if_using_gateway(system) and not
                self.check_if_tunnel_is_up(system)
            ):
                self.logger.error('{0} | System not reachable!'.format(system))
                raise SFTPSessionError

            # Get an sftp session
            sftp_session = self.get_sftp_session(system)
            if not sftp_session:
                raise SFTPSessionError('Cannot open an SFTP session to {0}'
                                       .format(system))
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
                    ) or '{0} | Missing logs!'.format(system)
        except (IOError, SFTPSessionError):
            result_data = pd.DataFrame()
            result_logs = 'Could not get information from this system'

        self.logger.debug('{0} | Consolidating results'.format(system))
        self.data = df_tools.consolidate_data(result_data,
                                              dataframe=self.data,
                                              system=system)
        self.logs[system] = result_logs
        self.results_queue.put(system)

    def _run_systemwide(self, target, *args):
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
            self.logger.info('{0} | Done collecting data!'
                             .format(self.results_queue.get()))

    def _threaded_handler(self):
        """
        Initialize tunnels and collect data&logs, threaded mode
        """
        with self:  # calls init_tunnels
            self._run_systemwide(self.get_data_and_logs)

    def _serial_handler(self):
        """
        Get data&logs. Serial (legacy) handler, working inside a for loop
        """
        for system in self.systems:
            self.logger.info('{0} | Initializing tunnel'.format(system))
            try:
                self.init_tunnels(system=system)
                self.get_data_and_logs(system=system)
            except (sshtunnel.BaseSSHTunnelForwarderError,
                    IOError,
                    SFTPSessionError):
                self.logger.warning('Continue to next system (if any)')
                continue
            finally:
                self.stop_server()

    def start(self):
        """
        Main method for the data collection
        """
        try:
            if self.safe:
                self._serial_handler()
            else:
                self._threaded_handler()
        except (sshtunnel.BaseSSHTunnelForwarderError, AttributeError) as exc:
            self.logger.error('Could not initialize the SSH tunnels, '
                              'aborting ({0})'.format(repr(exc)))
        except SSHException:
            self.logger.error('Could not open remote connection')
        except Exception as exc:
            self.logger.exception(exc)

    def to_pickle(self, name, compress=False, version=None):
        """
        Save collector object to [optionally] gzipped pickle.
        The pickle protocol used by default is the highest supported by the
        platform.

        Arguments:
            name (str): Name of the output file
            compress (boolean):
                Whether or not compress (deflate) the pickle file, defaults to
                ``False``
            version (int):
                pickle version, defaults to :const:`cPickle.HIGHEST_PROTOCOL`
        """
        buffer_object = six.BytesIO()
        cPickle.dump(obj=self,
                     file=buffer_object,
                     protocol=version or cPickle.HIGHEST_PROTOCOL)
        buffer_object.flush()
        if name.endswith('.gz'):
            compress = True
            name = name.rsplit('.gz')[0]  # will append the .gz extension below

        if compress:
            output = gzip
            name = "{0}.gz".format(name)
        else:
            output = builtins

        with output.open(name, 'wb') as pkl_out:
            pkl_out.write(buffer_object.getvalue())
        buffer_object.close()

    def _load_zipfile(self, zip_file, sftp_session=None):
        """
        Inflate a zip file and call :meth:`get_stats_from_host` with the
        decompressed CSV files
        """
        temp_dir = tempfile.gettempdir()
        self.logger.info('Decompressing ZIP file {0} into {1}...'
                         .format(zip_file, temp_dir))
        _df = pd.DataFrame()
        if not isinstance(sftp_session, SFTPClient):
            sftp_session = builtins  # open local file
        with sftp_session.open(zip_file, 'rb') as file_descriptor:
            c = six.BytesIO()
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
            self.logger.error('Bad ZIP file: {0}'.format(zip_file))
            self.logger.exception(exc)
        finally:
            for a_file in decompressed_files:
                self.logger.debug('Deleting file {0}'.format(a_file))
                os.remove(a_file)
            c.close()

        return _df


def load_zipfile(zipfile, system=None):
    """
    Load T4-CSV files contained inside a zip archive

    Arguments:
        zipfile
        system (Optional[str]):
            Hostname where the zip file is located, None for local filesystem
    Return:
        ``pandas.DataFrame``
    """
    col = Collector(alldays=True, nologs=True)
    return col.get_stats_from_host(zipfile, hostname=system, compressed=True)


def read_pickle(name, compress=False, logger=None):
    """
    Restore dataframe plus its metadata from (optionally deflated) pickle store

    Arguments:
        name(str): Input file name
        compress (Optional[boolean]):
            Whether or not the file is compressed (``True`` if file extension
            ends with '.gz'). Defaults to ``False``.
        logger (Optional[logging.Logger]): Optional logger object
    Return:
        ``Collector``
    """
    if compress or name.endswith('.gz'):
        mode = gzip
    else:
        mode = builtins

    with mode.open(name, 'rb') as picklein:
        collector_ = cPickle.load(picklein)
    if logger:
        collector_.logger = logger
    collector_.logger = logger or init_logger()
    return collector_


def __from_t4csv(*args, **kwargs):
    return df_tools.reload_from_csv(*args, **kwargs)


def add_methods_to_pandas_dataframe(logger=None):
    """
    Add custom methods to pandas.DataFrame, allowing for example running
    :meth:`t4mon.calculations.apply_calcs` or
    :meth:`t4mon.calculations.clean_calcs` directly from any pandas DataFrame

    Arguments:
        logger (Optional[logging.Logger]): Optional logger object
    """
    pd.DataFrame.oper = calculations.oper
    pd.DataFrame.oper_wrapper = calculations.oper_wrapper
    pd.DataFrame.recursive_lis = calculations.recursive_lis
    pd.DataFrame.apply_calcs = calculations.apply_calcs
    pd.DataFrame.clean_calcs = calculations.clean_calcs
    pd.DataFrame.logger = logger or init_logger()
    pd.DataFrame.select_var = df_tools.select
    pd.DataFrame.plot_var = gen_plot.plot_var
    pd.DataFrame.from_t4csv = __from_t4csv
    pd.DataFrame.from_t4zip = load_zipfile
    pd.DataFrame.to_t4csv = df_tools.dataframe_to_t4csv
