#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import logging
import datetime as dt
from functools import wraps
from multiprocessing import Pool

import six
import types
import pandas as pd

from . import arguments
from .collector import Collector, read_pickle
from .logger import init_logger
from .df_tools import reload_from_csv, consolidate_data
from .gen_report import gen_report

__all__ = ('Orchestrator')


# Make the Orchestrator class picklable, required by Pool.map()
def _pickle_method(method):
    if six.PY2:
        func_name = method.im_func.__name__
        obj = method.im_self
        cls = method.im_class
    else:
        func_name = method.__func__.__name__
        obj = method.__self__
        cls = method.__class__
    return _unpickle_method, (func_name, obj, cls)


def _unpickle_method(func_name, obj, cls):
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
    It represents the object passed to jinja2 containing:

        - data: dataframe passed to get_graphs()
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - system: string containing current system-id being rendered

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
        self.reports_written = []
        self.settings_file = settings_file or arguments.DEFAULT_SETTINGS_FILE
        self.safe = safe
        self.reports_folder = None
        self.store_folder = None
        self.systems = None
        self.kwargs = kwargs

        self.set_folders()

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

    def check_folders(self):
        """
        Check during runtime if all destination folders are in place:

        - reports output folder (self.reports_folder)
        - CSV and DAT store folder (self.store_folder)
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

    def check_external_files_from_config(self, add_external_to_object=True):
        """
        Read the settings file and check if external config files (i.e.
        calculations_file, html_template) are defined and exist in the
        filesystem.
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

    def check_files(self):
        """
        Check during runtime if all required files exist and are readable:

        - settings file
        - calculations file (settings/MISC/calculations_file)
        - Jinja template (settings/MISC/html_template)
        - graphs definition file (settings/MISC/graphs_definition_file)
        - reports output folder (self.reports_folder)
        - CSV and DAT store folder (self.store_folder)

        Raises arguments.ConfigReadError if not everything is in place
        """
        if not os.path.isfile(self.settings_file):
            self.logger.critical('Settings file not found: {0}'
                                 .format(self.settings_file))
            raise arguments.ConfigReadError
        try:
            self.check_external_files_from_config()
        except (arguments.ConfigReadError,
                six.moves.configparser.Error) as _exc:
            self.logger.exception(_exc)
            raise arguments.ConfigReadError

    def _check_files(func):
        """ Decorator wrapper for check_files """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            self.check_files()
            return func(self, *args, **kwargs)
        return wrapped

    def _check_folders(func):
        """ Decorator wrapper for check_folders """
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            self.check_folders()
            return func(self, *args, **kwargs)
        return wrapped

    @_check_files
    def set_folders(self):
        """Read reports and store folders and systems from settings file"""
        self.logger.debug('Using settings file: {0}'
                          .format(self.settings_file))
        conf = arguments.read_config(self.settings_file)
        self.reports_folder = conf.get('MISC', 'reports_folder') or './reports'
        self.store_folder = conf.get('MISC', 'store_folder') or './store'
        self.systems = [item for item in conf.sections()
                        if item not in ['GATEWAY', 'MISC']]

    def date_tag(self):
        """
        Convert self.date_time to the filesystem friendly format '%Y%m%d_%H%M'
        """
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        return dt.date.strftime(current_date,
                                "%Y%m%d_%H%M")

    @_check_folders
    def create_report(self, system=None):
        """
        Create a single report for a particular system
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

    def reports_generator(self):
        """
        Call jinja2 template, separately to safely store the logs
        in case of error.
        Doing this with a multiprocessing pool instead of threads in order to
        avoid problems with GC and matplotlib backends particularly with
        Windows environments.
        """
        if self.safe:
            for system in self.systems:
                self.reports_written.append(self.create_report(system))
        else:
            # make the collector picklable
            # _logger = self.logger
            # self.logger = None
            pool = Pool(processes=len(self.systems))
            written = pool.map(self.create_report, self.systems)
            self.reports_written.extend(written)
            pool.close()
            # self.logger = _logger

    @_check_folders
    def local_store(self, col):
        """
        Make a local copy of the current data in CSV and gzipped pickle
        """
        self.logger.info('Making a local copy of data in store folder: ')
        destfile = '{0}/data_{1}.pkl'.format(self.store_folder,
                                             self.date_tag())
        col.to_pickle(destfile, compress=True)
        self.logger.info('  -->  {0}.gz'.format(destfile))
        destfile = '{0}/data_{1}.csv'.format(self.store_folder,
                                             self.date_tag())
        col.data.to_csv(destfile)
        self.logger.info('  -->  {0}'.format(destfile))

        # Write logs
        if not col.nologs:
            for system in col.systems:
                if system not in col.logs:
                    self.logger.warning('No log info found for {0}'
                                        .format(system))
                    continue
                with open('{0}/logs_{1}_{2}.txt'.format(self.store_folder,
                                                        system,
                                                        self.date_tag()),
                          'w') as logtxt:
                    logtxt.writelines(self.logs[system])

    @_check_files
    def start(self):  # pragma: no cover
        """
        Main method, get data and logs, store and render the HTML output
        """
        # Open the connection and gather all data and logs
        _collector = Collector(
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
        self.local_store(_collector)

        # Generate reports
        if self.noreports:
            self.logger.info('Skipped report generation')
        else:
            self.reports_generator()

        self.logger.warning('Done!')

    @_check_files
    def create_reports_from_local(self,
                                  data_file,
                                  pkl=True,
                                  plain=False,
                                  system=None,
                                  **kwargs):
        """
        Generate HTML files from data stored locally
        """
        # load the input file
        if not os.path.exists(data_file):
            self.logger.error('{0} file {1} cannot be found'
                              .format('PKL' if pkl else 'CSV', data_file))
            raise IOError
        if pkl:
            _collector = read_pickle(data_file, logger=self.logger)
            self.data = _collector.data
            self.logs = _collector.logs
            if system:
                self.systems = system if isinstance(system, list) else [system]
            else:
                self.systems = _collector.systems
        else:  # CSV
            if not system:
                system = os.path.splitext(os.path.basename(data_file))[0]
            self.data = reload_from_csv(data_file,
                                        plain=plain)
            self.data = consolidate_data(self.data,
                                         system=system)
            self.systems = system if isinstance(system, list) else [system]
        # Populate the log info with fake data
        for system in self.systems:
            self.logs[system] = 'Log collection omitted for '\
                                'locally generated reports at '\
                                '{0} for {1}'.format(self.date_tag(), system)
            self.logger.info(self.logs[system])

        # Create the reports
        self.reports_generator()
        self.logger.info('Done!')

if six.PY2:
    six.moves.copyreg.pickle(types.MethodType,
                             _pickle_method,
                             _unpickle_method)
else:
    six.moves.copyreg.pickle(Orchestrator,
                             _pickle_method,
                             _unpickle_method)
