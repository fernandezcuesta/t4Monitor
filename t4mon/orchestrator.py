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

# import threading

from matplotlib import pylab as pylab  # isort:skip
from matplotlib import pyplot as plt  # isort:skip

from . import collector  # isort:skip
from .logger import DEFAULT_LOGLEVEL, init_logger  # isort:skip
from .gen_report import gen_report  # isort:skip
from .df_tools import consolidate_data, reload_from_csv  # isort:skip


__all__ = ('Orchestrator')


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

    """ Object passed to jinja2 containing:

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """

    def __init__(self,
                 logger=None,
                 loglevel=None,
                 noreports=False,
                 settings_file=None,
                 safe=False,
                 **kwargs):
        self.calculations_file = ''
        self.date_time = dt.date.strftime(dt.datetime.today(),
                                          "%d/%m/%Y %H:%M:%S")
        self.date_tag = self.date_tag()
        self.graphs = {}  # will be filled by calls from within jinja for loop
        self.graphs_definition_file = ''
        self.html_template = ''
        self.loglevel = loglevel
        self.logger = logger or init_logger(self.loglevel)
        self.noreports = noreports
        self.reports_written = []
        self.reports_folder = './reports'
        self.settings_file = settings_file or collector.DEFAULT_SETTINGS_FILE
        self.store_folder = './store'
        self.safe = safe
        self.kwargs = kwargs
        self._index = 0
        self.data = pd.DataFrame()
        self.logs = {}
        self.systems = [item for item in
                        collector.read_config(self.settings_file).sections()
                        if item not in ['GATEWAY', 'MISC']]
        self.check_files()

    def __str__(self):
        return ('Orchestrator object created on {0} with loglevel {1}\n'
                'graphs_definition_file: {2}\n'
                'html_template: {3}\n'
                'reports folder: {4}\n'
                'store folder: {5}\n'
                'data size: {6}\n'
                'settings_file: {7}\n'
                'calculations_file: {8}\n'
                'mode: {9}'.format(
                    self.date_time,
                    logging.getLevelName(self.logger.level),
                    self.graphs_definition_file,
                    self.html_template,
                    self.reports_folder,
                    self.store_folder,
                    self.data.shape,
                    self.settings_file,
                    self.calculations_file,
                    'safe' if self.safe else 'fast'
                ))

    def __iter__(self):
        return self

    def next(self):  # Python 3: def __next__(self)
        if self._index >= len(self.systems):
            raise StopIteration
        else:
            self._index += 1
            return self

    def get_absolute_path(self, filename=''):
        """
        Return the absolute path if relative to the settings file location
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
        Orchestrator objects
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

    def check_folders(self):
        """
        Runtime method checking if all destination folders are in place:

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
        Runtime method checking if all required files exist and are readable:

        - settings file
        - calculations file (settings/MISC/calculations_file)
        - Jinja template (settings/MISC/html_template)
        - graphs definition file (settings/MISC/graphs_definition_file)
        - reports output folder (self.reports_folder)
        - CSV and DAT store folder (self.store_folder)

        Returns: Boolean (whether or not everything is in place)
        """
        if not os.path.isfile(self.settings_file):
            self.logger.critical('Settings file not found: %s',
                                 self.settings_file)
            raise collector.ConfigReadError
        try:
            self.get_external_files_from_config()
        except (collector.ConfigReadError,
                collector.ConfigParser.Error) as _exc:
            self.logger.exception(_exc)
            raise collector.ConfigReadError

        # Check that destination folders are in place
        self.check_folders()

        for option in ['graphs_definition_file',
                       'html_template',
                       'calculations_file']:
            try:
                self_attribute = self.__getattribute__(option)
            except AttributeError:
                raise collector.ConfigReadError
            self.logger.info('Checking %s: %s', option, self_attribute)
            if not self_attribute or not os.path.isfile(self_attribute):
                self.logger.critical('%s NOT found: %s',
                                     option,
                                     self_attribute)
                raise collector.ConfigReadError

    def date_tag(self):
        """ Converts self.date_time to the format '%Y%m%d_%H%M' """
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        return dt.date.strftime(current_date,
                                "%Y%m%d_%H%M")

    def create_report(self, system=None, logger=None):
        """ Method for creating a single report for a particular system """
        if not system:
            raise AttributeError('Need a value for system!')
        logger = logger or init_logger(self.loglevel)
        report_name = '{0}/Report_{1}_{2}.html'.format(self.reports_folder,
                                                       self.date_tag,
                                                       system)
        logger.debug('%s | Generating HTML report (%s)',
                     system,
                     report_name)
        with open(report_name, 'w') as output:
            output.writelines(gen_report(container=self,
                                         system=system))
        return report_name

    # def reports_generator(self):
    #     """
    #     Call jinja2 template, separately to safely store the logs
    #     in case of error.
    #     Doing this with threads throws many errors with Qt (when acting as
    #     matplotlib backend out from main thread)
    #     """
    #     # Initialize default figure sizes and styling
    #     pylab.rcParams['figure.figsize'] = 13, 10

    #     plt.style.use('ggplot')

    #     if self.safe:
    #         for system in self.collector.systems:
    #             self.create_report(system)
    #     else:
    #         threads = [
    #             threading.Thread(target=self.create_report,
    #                              kwargs={'system': system},
    #                              name=system)
    #             for system in self.collector.systems
    #         ]
    #         for thread_item in threads:
    #             thread_item.daemon = True
    #             thread_item.start()
    #         for thread_item in threads:  # Assuming all take the same time
    #             thread_item.join()

    def reports_generator(self):
        """
        Call jinja2 template, separately to safely store the logs
        in case of error.
        Doing this with multiple processes to avoid problems with GC and
        matplotlib backends under Windows environments.
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
        Makes a local copy of the current data in CSV and gzipped pickle
        """
        self.logger.info('Making a local copy of data in store folder: ')
        destfile = '{0}/data_{1}.pkl'.format(self.store_folder, self.date_tag)
        col.to_pickle(destfile, compress=True)
        self.logger.info('  -->  %s.gz', destfile)
        destfile = '{0}/data_{1}.csv'.format(self.store_folder, self.date_tag)
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
                                                        self.date_tag),
                          'w') as logtxt:
                    logtxt.writelines(self.logs[system])

    def start(self):  # pragma: no cover
        """ Main method, gets data and logs, store and render the HTML output
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

        self.logger.info('Done!')

    def create_reports_from_local(self, data_file,
                                  pkl=True,
                                  plain=False,
                                  system=None):
        """ Generate HTML files from data stored locally """
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
                                '{} for {}'.format(self.date_tag, system)
            self.logger.info(self.logs[system])

        # Create the reports
        self.reports_generator()
        self.logger.info('Done!')

    # API CHANGE ON 0.12 MAKES THIS OBSOLETE
    # def set_threaded_mode(self, safe=False):
        # """ Change both orchestrator and collector mode (serial/threaded) """
        # self.safe = self.collector.safe = safe
        # self.logger.debug('Changed to %s mode',
                          # 'safe' if safe else 'fast')

    # def set_settings_file(self, settings_file=None):
        # """ Change the settings file both for orchestrator and collector """
        # self.settings_file = settings_file or collector.DEFAULT_SETTINGS_FILE
        # self.collector.settings_file = self.settings_file
        # self.collector.conf = collector.read_config(self.settings_file)

    # def set_logger_level(self, loglevel=None):
        # """ Change the loglevel for orchestrator and collector objects """
        # if not loglevel:
            # loglevel = DEFAULT_LOGLEVEL
        # self.logger.setLevel(loglevel)
        # self.collector.logger.setLevel(loglevel)


def create_simple_report(self, logger=None):
    """ Method for creating a single report for a particular system """
    logger = logger or init_logger(self.loglevel)
    logger.critical(self._index)
    system = self.systems[self._index]
    if not system:
        raise AttributeError('Need a value for system!')
    report_name = '{0}/Report_{1}_{2}.html'.format(self.reports_folder,
                                                   self.date_tag,
                                                   system)
    logger.debug('%s | Generating HTML report (%s)',
                 system,
                 report_name)
    with open(report_name, 'w') as output:
        output.writelines(gen_report(container=self,
                                     system=system,
                                     logger=logger))
    return report_name
