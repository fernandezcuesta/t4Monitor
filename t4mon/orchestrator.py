#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import sys
import types
import logging
import datetime as dt
from functools import wraps
from multiprocessing import Pool

import six
import pandas as pd

from . import df_tools, arguments, collector
from .logger import init_logger
from .gen_report import gen_report


# Make the Orchestrator class picklable, required by Pool.map()
def _pickle_method(method):  # pragma: no cover
    if six.PY2:
        func_name = method.im_func.__name__
        obj = method.im_self
        cls = method.im_class
    else:
        func_name = method.__func__.__name__
        obj = method.__self__
        cls = method.__class__
    return _unpickle_method, (func_name, obj, cls)


def _unpickle_method(func_name, obj, cls):  # pragma: no cover
    for cls in cls.mro():
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)


class Orchestrator(object):

    """
    Class orchestrating the retrieval, local storage and report generation
    It represents the object passed to :func:`~gen_report`.

    Keyword Arguments:
        logger (logging.Logger):
            logging instance
        loglevel (str):
            logger level, if no ``logger`` passed
        noreports (boolean or False):
            skip report (HTML) generation
        settings_file (str or :const:`~arguments.DEFAULT_SETTINGS_FILE`):
            Settings filename
        safe (boolean or False):
            Serial (slower) mode, not using threads or multiple processes

    Attributes:
        data (pandas.DataFrame): data retrieved from the remote hosts
        date_time (str): data collection date in ``%d/%m/%Y %H:%M:%S`` format
        loglevel (str): level as passed from ``loglevel`` argument
        logger (logging.Logger): logging instance as passed from ``logger``
        logs (dict): output of remote command execution on each system
        noreports (boolean): flag indicating to skip report generation, as per
            ``noreports`` argument
        reports_folder (str): output folder for reports as per
            ``settings_file``
        reports_written (list): finished report's filenames
        settings_file (str): settings file as ``settings_file`` argument
        safe (boolean): flag indicating safe/threaded mode (``safe``` argument)
        store_folder (str): output folder for retrieved data as per
            ``settings_file``
    """

    def __init__(self,
                 logger=None,
                 loglevel=None,
                 noreports=False,
                 settings_file=None,
                 safe=False,
                 **kwargs):
        self.data = pd.DataFrame()
        self.date_time = dt.date.strftime(dt.datetime.today(),
                                          "%d/%m/%Y %H:%M:%S")
        self.loglevel = loglevel
        self.logger = logger or init_logger(self.loglevel)
        self.logs = {}
        self.noreports = noreports
        self.reports_folder = None
        self.reports_written = []
        self.settings_file = settings_file or arguments.DEFAULT_SETTINGS_FILE
        self.safe = safe
        self.store_folder = None
        self.systems = None
        self.kwargs = kwargs

        self._set_folders()

    def __str__(self):
        return ('Orchestrator object created on {0} with loglevel {1}\n'
                'reports folder: {2}\n'
                'store folder: {3}\n'
                'data size: {4}\n'
                'settings_file: {5}\n'
                'mode: {6}'.format(
                    self.date_time,
                    logging.getLevelName(self.logger.level),
                    self.reports_folder,
                    self.store_folder,
                    self.data.shape,
                    self.settings_file,
                    'safe' if self.safe else 'fast'
                ))

    def __getstate__(self):
        """
        """
        odict = self.__dict__.copy()
        odict['loggername'] = self.logger.name
        del odict['logger']
        return odict

    def __setstate__(self, state):
        """
        """
        state['logger'] = init_logger(state.get('loggername'))
        self.__dict__.update(state)

    def _check_folders(self):
        """
        Runtime check if all destination folders are in place
        """
        for folder in ['store', 'reports']:
            try:
                value = getattr(self, '{0}_folder'.format(folder))
                self.logger.debug('Using {0} folder: {1}'
                                  .format(folder, value))
                os.makedirs(value)
            except OSError:
                self.logger.debug('{0} folder already exists: {1}'
                                  .format(folder, os.path.abspath(value)))

    def _check_external_files_from_config(self, add_external_to_object=True):
        """
        Read the settings file and check if external config files (i.e.
        ``calculations_file``, ``html_template``) are defined and exist.
        Optionally add all the external files to the Orchestrator object.
        """
        conf = arguments.read_config(self.settings_file)
        for external_file in ['calculations_file',
                              'html_template',
                              'graphs_definition_file']:

            # Check if all external files are properly configured
            if conf.has_option('MISC', external_file):
                value = arguments.get_absolute_path(conf.get('MISC',
                                                             external_file),
                                                    self.settings_file)
            else:
                _msg = 'Entry for {0} not found in MISC section (file: {1})'\
                    .format(external_file, self.settings_file)
                raise arguments.ConfigReadError(_msg)

            # Check if external files do exist
            if not os.path.isfile(value):
                _msg = '{0} NOT found: {1}'.format(external_file, value)
                raise arguments.ConfigReadError(_msg)

            # Update Orchestrator object
            if add_external_to_object:
                self.__setattr__(external_file, value)

    def _check_files(self):
        """
        Runtime check that all required files exist and are readable
        """
        if not os.path.isfile(self.settings_file):
            self.logger.critical('Settings file not found: {0}'
                                 .format(self.settings_file))
            raise arguments.ConfigReadError
        try:
            self._check_external_files_from_config()
        except (arguments.ConfigReadError,
                six.moves.configparser.Error) as _exc:
            self.logger.exception(_exc)
            raise arguments.ConfigReadError

    def check_files(func):
        """
        Decorator checking during runtime if all required files exist and are
        readable:

        - settings file
        - calculations file (``settings/MISC/calculations_file``)
        - Jinja template (``settings/MISC/html_template``)
        - graphs definition file (``settings/MISC/graphs_definition_file``)
        - reports output folder (``self.reports_folder``)
        - CSV and DAT store folder (``self.store_folder``)

        Raises arguments.ConfigReadError if not everything is in place
        """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            self._check_files()
            return func(self, *args, **kwargs)
        return wrapped

    def check_folders(func):
        """
        Decorator checking during runtime if all destination folders are in
        place:

        - reports output folder (:attr:`reports_folder`)
        - CSV and DAT store folder (:attr:`store_folder`)
        """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            self._check_folders()
            return func(self, *args, **kwargs)
        return wrapped

    @check_files
    def _set_folders(self):
        """Setter for reports and store folders and systems"""
        self.logger.debug('Using settings file: {0}'
                          .format(self.settings_file))
        conf = arguments.read_config(self.settings_file)
        if six.PY3:
            self.reports_folder = conf.get('MISC',
                                           'reports_folder',
                                           fallback='./reports')
            self.store_folder = conf.get('MISC',
                                         'store_folder',
                                         fallback='./store')
        else:
            self.reports_folder = conf.get('MISC',
                                           'reports_folder',
                                           './reports')
            self.reports_folder = conf.get('MISC',
                                           'reports_folder',
                                           './reports')

        self.systems = [item for item in conf.sections()
                        if item not in ['GATEWAY', 'MISC']]

    def date_tag(self):
        """
        Get a filesystem friendly representation of :attr:`date_time` in the
        format ``%Y%m%d_%H%M``.

        Return: str
        """
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        return dt.date.strftime(current_date,
                                "%Y%m%d_%H%M")

    @check_folders
    def create_report(self, system=None):
        """
        Create a single report for a particular system, returing the report
        name.

        Keyword Arguments:
            system (str): System for which create the report

        Return: str
        """
        # TODO: allow creating a common report (comparison)
        if not system:
            raise AttributeError('Need a value for system!')
        report_name = '{0}/Report_{1}_{2}.html'.format(self.reports_folder,
                                                       self.date_tag(),
                                                       system)
        self.logger.debug('{0} | Generating HTML report ({1})'
                          .format(system, report_name))
        with open(report_name, 'w') as output:
            output.writelines(gen_report(container=self,
                                         system=system))
        return report_name

    def _reports_generator(self):
        """
        Call Jinja2 template, separately to safely store the logs in case of
        error.
        Doing this with a multiprocessing pool instead of threads in order to
        avoid problems with GC and matplotlib backends particularly with
        Windows environments.
        """
        if self.safe:
            for system in self.systems:
                self.reports_written.append(self.create_report(system))
        else:
            pool = Pool(processes=len(self.systems))
            written = pool.map(self.create_report, self.systems)
            self.reports_written.extend(written)
            pool.close()

    @check_folders
    def _local_store(self, collector):
        """
        Make a local copy of the current data in CSV and gzipped pickle
        """
        self.logger.info('Making a local copy of data in store folder: ')
        destfile = '{0}/data_{1}.pkl'.format(self.store_folder,
                                             self.date_tag())
        collector.to_pickle(destfile, compress=True)
        self.logger.info('  -->  {0}.gz'.format(destfile))
        destfile = '{0}/data_{1}.csv'.format(self.store_folder,
                                             self.date_tag())
        collector.data.to_csv(destfile)
        self.logger.info('  -->  {0}'.format(destfile))

        # Write logs
        if not collector.nologs:
            for system in collector.systems:
                if system not in collector.logs:
                    self.logger.warning('No log info found for {0}'
                                        .format(system))
                    continue
                with open('{0}/logs_{1}_{2}.txt'.format(self.store_folder,
                                                        system,
                                                        self.date_tag()),
                          'w') as logtxt:
                    logtxt.writelines(self.logs[system])

    @check_files
    def start(self):  # pragma: no cover
        """
        Get data and logs from remote hosts, store the results and render the
        HTML reports
        """
        # Open the connection and gather all data and logs
        _collector = collector.Collector(
            logger=self.logger,
            settings_file=self.settings_file,
            safe=self.safe,
            **self.kwargs
        )

        _collector.start()
        self.data = _collector.data
        self.logs = _collector.logs
        self.systems = _collector.systems

        if self.data.empty:
            self.logger.critical('Could not retrieve data!!! Aborting.')
            return

        # Store the data locally
        self._local_store(_collector)

        # Generate reports
        if self.noreports:
            self.logger.info('Skipped report generation')
        else:
            self._reports_generator()

        self.logger.warning('Done!')

    @check_files
    def create_reports_from_local(self,
                                  data_file,
                                  pkl=True,
                                  plain=False,
                                  system=None,
                                  **kwargs):
        """
        Generate HTML files from data stored locally

        Arguments:
            data_file (str):
                Data filename
        Keyword Arguments:
            pkl (boolean or True):
                indicate if data is a pickled dataframe or a CSV
            plain (boolean or False):
                when ``pkl==False``, indicate if the CSV is a plain (aka excel
                format) or a T4-compliant CSV
            system (str):
                Indicate the system name of the input data, important when data
                comes in CSV format (``pkl==False``)
        """
        # load the input file
        if not os.path.exists(data_file):
            self.logger.error('{0} file {1} cannot be found'
                              .format('PKL' if pkl else 'CSV', data_file))
            raise IOError
        if pkl:
            _collector = collector.read_pickle(data_file, logger=self.logger)
            self.data = _collector.data
            self.logs = _collector.logs
            if system:
                self.systems = system if isinstance(system, list) else [system]
            else:
                self.systems = _collector.systems
        else:  # CSV
            if not system:
                system = os.path.splitext(os.path.basename(data_file))[0]
            self.data = df_tools.reload_from_csv(data_file,
                                                 plain=plain)
            self.data = df_tools.consolidate_data(self.data,
                                                  system=system)
            self.systems = system if isinstance(system, list) else [system]
        # Populate the log info with fake data
        for system in self.systems:
            self.logs[system] = 'Log collection omitted for '\
                                'locally generated reports at '\
                                '{0} for {1}'.format(self.date_tag(), system)
            self.logger.info(self.logs[system])

        # Create the reports
        self._reports_generator()
        self.logger.info('Done!')


if sys.version_info < (3, 5):
    six.moves.copyreg.pickle(types.MethodType,
                             _pickle_method,
                             _unpickle_method)
else:
    six.moves.copyreg.pickle(Orchestrator,
                             _pickle_method,
                             _unpickle_method)
