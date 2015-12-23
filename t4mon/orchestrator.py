#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

import datetime as dt
import logging
import os

import copy_reg
import pandas as pd
import types
from multiprocessing import Pool

from . import collector  # isort:skip
from .logger import init_logger  # isort:skip
from .gen_report import gen_report  # isort:skip
from .df_tools import consolidate_data, reload_from_csv  # isort:skip


__all__ = ('Orchestrator')


# Make the Orchestrator class pickable, required by Pool.map()
def _pickle_method(method):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
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

copy_reg.pickle(types.MethodType, _pickle_method, _unpickle_method)


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
        self.reports_folder = './reports'
        self.settings_file = settings_file or collector.DEFAULT_SETTINGS_FILE
        self.store_folder = './store'
        self.safe = safe
        self.kwargs = kwargs
        self.systems = [item for item in
                        collector.read_config(self.settings_file).sections()
                        if item not in ['GATEWAY', 'MISC']]
        self.check_files()

    def __str__(self):
        return ('Orchestrator object created on {} with loglevel {}\n'
                'reports folder: {}\n'
                'store folder: {}\n'
                'data size: {}\n'
                'settings_file: {}\n'
                'mode: {}'.format(
                    self.date_time,
                    logging.getLevelName(self.logger.level),
                    self.reports_folder,
                    self.store_folder,
                    self.data.shape,
                    self.settings_file,
                    'safe' if self.safe else 'fast'
                ))

    def get_absolute_path(self, filename=''):
        """
        Get the absolute path if relative to the settings file location
        """
        if os.path.isabs(filename):
            return filename
        else:
            relpath = os.path.dirname(os.path.abspath(self.settings_file))
            return '{}{}{}'.format(relpath,
                                   os.sep if relpath != os.sep else '',
                                   filename)

    def get_external_files_from_config(self):
        """
        Read the settings file and check if external config files (i.e.
        calculations_file, html_template) are defined and add them to the
        Orchestrator object as attributes
        """
        conf = collector.read_config(self.settings_file)
        self.logger.debug('Using settings file: %s', self.settings_file)
        for option in ['calculations_file',
                       'html_template',
                       'graphs_definition_file',
                       'reports_folder',
                       'store_folder']:
            if conf.has_option('MISC', option):
                self.__setattr__(
                    option,
                    self.get_absolute_path(conf.get('MISC', option))
                )

    def check_external_files_from_config(self):
        """
        Read the settings file and check if external config files (i.e.
        calculations_file, html_template) are defined and exist in the
        filesystem
        """
        conf = collector.read_config(self.settings_file)
        self.logger.debug('Using settings file: %s', self.settings_file)
        for option in ['calculations_file',
                       'html_template',
                       'graphs_definition_file',
                       'reports_folder',
                       'store_folder']:
            value = self.get_absolute_path(conf.get('MISC', option))
            if option in self.__dict__:  # update valid values
                self.__setattr__(option, value)
            elif not os.path.isfile(value):
                self.logger.critical('%s NOT found: %s',
                                     option,
                                     value)
                raise collector.ConfigReadError

    def check_folders(self):
        """
        Check during runtime if all destination folders are in place:

        - reports output folder (self.reports_folder)
        - CSV and DAT store folder (self.store_folder)
        """
        # Create store folder if needed
        try:
            self.logger.debug('Using store folder: %s', self.store_folder)
            os.makedirs(self.store_folder)
        except OSError:
            self.logger.debug('Store folder already exists: %s',
                              os.path.abspath(self.store_folder))
        # Create reports folder if needed
        try:
            self.logger.debug('Using reports folder: %s', self.reports_folder)
            os.makedirs(self.reports_folder)
        except OSError:
            self.logger.debug('Reports folder already exists: %s',
                              os.path.abspath(self.reports_folder))

    def check_files(self):
        """
        Check during runtime if all required files exist and are readable:

        - settings file
        - calculations file (settings/MISC/calculations_file)
        - Jinja template (settings/MISC/html_template)
        - graphs definition file (settings/MISC/graphs_definition_file)
        - reports output folder (self.reports_folder)
        - CSV and DAT store folder (self.store_folder)

        Raises collector.ConfigReadError if not everything is in place
        """
        if not os.path.isfile(self.settings_file):
            self.logger.critical('Settings file not found: %s',
                                 self.settings_file)
            raise collector.ConfigReadError
        try:
            self.check_external_files_from_config()
        except (collector.ConfigReadError,
                collector.ConfigParser.Error) as _exc:
            self.logger.exception(_exc)
            raise collector.ConfigReadError
        # Check that destination folders are in place
        self.check_folders()

    def date_tag(self):
        """
        Convert self.date_time to the filesystem friendly format '%Y%m%d_%H%M'
        """
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        return dt.date.strftime(current_date,
                                "%Y%m%d_%H%M")

    def create_report(self, system=None, logger=None):
        """
        Create a single report for a particular system
        """
        # TODO: allow creating a common report (comparison)
        if not system:
            raise AttributeError('Need a value for system!')
        logger = logger or init_logger(self.loglevel)
        report_name = '{0}/Report_{1}_{2}.html'.format(self.reports_folder,
                                                       self.date_tag(),
                                                       system)
        logger.debug('%s | Generating HTML report (%s)',
                     system,
                     report_name)
        with open(report_name, 'w') as output:
            output.writelines(gen_report(container=self,
                                         system=system))
        return report_name

    def reports_generator(self):
        """
        Call jinja2 template, separately to safely store the logs
        in case of error.
        Doing this with a multiprocessing pool to avoid problems with GC and
        matplotlib backends under Windows environments
        """
        if self.safe:
            for system in self.systems:
                self.reports_written.append(self.create_report(system))
        else:
            # make the collector pickable
            _logger = self.logger
            self.logger = None
            pool = Pool(processes=len(self.systems))
            written = pool.map(self.create_report, self.systems)
            self.reports_written.extend(written)
            pool.close()
            self.logger = _logger

    def local_store(self, col):
        """
        Make a local copy of the current data in CSV and gzipped pickle
        """
        self.logger.info('Making a local copy of data in store folder: ')
        destfile = '{0}/data_{1}.pkl'.format(self.store_folder,
                                             self.date_tag())
        col.to_pickle(destfile, compress=True)
        self.logger.info('  -->  %s.gz', destfile)
        destfile = '{0}/data_{1}.csv'.format(self.store_folder,
                                             self.date_tag())
        col.data.to_csv(destfile)
        self.logger.info('  -->  %s', destfile)

        # Write logs
        if not col.nologs:
            for system in col.systems:
                if system not in col.logs:
                    self.logger.warning('No log info found for %s', system)
                    continue
                with open('{0}/logs_{1}_{2}.txt'.format(self.store_folder,
                                                        system,
                                                        self.date_tag()),
                          'w') as logtxt:
                    logtxt.writelines(self.logs[system])

    def start(self):  # pragma: no cover
        """
        Main method, get data and logs, store and render the HTML output
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
        self.local_store(_collector)

        # Generate reports
        if self.noreports:
            self.logger.info('Skipped report generation')
        else:
            self.reports_generator()

        self.logger.warning('Done!')

    def create_reports_from_local(self, data_file,
                                  pkl=True,
                                  plain=False,
                                  system=None):
        """
        Generate HTML files from data stored locally
        """
        # load the input file
        if not os.path.exists(data_file):
            self.logger.error('%s file %s cannot be found',
                              'PKL' if pkl else 'CSV',
                              data_file)
            raise IOError
        if pkl:
            _collector = collector.read_pickle(data_file,
                                               logger=self.logger)
            self.data = _collector.data
            self.logs = _collector.logs
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
                                '{} for {}'.format(self.date_tag(), system)
            self.logger.info(self.logs[system])

        # Create the reports
        self.reports_generator()
        self.logger.info('Done!')
