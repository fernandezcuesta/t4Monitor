#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import

import os
import datetime as dt
import threading

import matplotlib  # isort:skip
# Set matplotlib's backend before first import of pyplot or pylab,
# Qt4 doesn't like threads
if os.name == 'posix':
    matplotlib.use('Cairo')
else:
    matplotlib.use('TkAgg')
# Required by matplotlib when using TkAgg backend
from matplotlib import pylab as pylab  # isort:skip
from matplotlib import pyplot as plt  # isort:skip

from . import collector  # isort:skip
from .logger import DEFAULT_LOGLEVEL, init_logger  # isort:skip
from .gen_report import gen_report  # isort:skip
from .arguments_parser import parse_arguments  # isort:skip


__all__ = ('Orchestrator')


class Orchestrator(object):
    """ Object passed to jinja2 containing:

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """

    def __init__(self,
                 logger=None,  # shall I remove this???
                 loglevel=None,
                 settings_file=None,
                 threaded=False,
                 **kwargs):
        self.calculations_file = ''
        self.data = collector.pd.DataFrame()
        self.date_time = dt.date.strftime(dt.datetime.today(),
                                          "%d/%m/%Y %H:%M:%S")
        self.graphs = {}  # will be filled by calls from within jinja for loop
        self.graphs_file = ''
        self.html_template = ''
        self.logger = logger if logger else init_logger(
                      loglevel if loglevel else DEFAULT_LOGLEVEL
                      )
        self.logs = {}
        self.reports_written = []
        self.reports_folder = './reports'
        self.settings_file = settings_file or collector.DEFAULT_SETTINGS_FILE
        self.store_folder = './store'
        self.system = ''
        self.threaded = threaded
        self.year = dt.date.today().year
        self.check_files()
        self.kwargs = kwargs

    def __str__(self):
        return ('Container created on {0} for system: "{1}"\nLoglevel: {2}\n'
                'graphs_file: {3}\nhtml_template: {4}\nreports folder: {5}\n'
                'store folder: {6}\ndata size: {7}\nsettings_file: {8}\n'
                'calculations_file: {9}'.format(
                    self.date_time,
                    self.system,
                    self.logger.level,
                    self.graphs_file,
                    self.html_template,
                    self.reports_folder,
                    self.store_folder,
                    self.data.shape,
                    self.settings_file,
                    self.calculations_file
                ))

# TODO: get rid of this?
    def clone(self, system=''):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
        """
        my_clone = Orchestrator(logger=self.logger)
        my_clone.calculations_file = self.calculations_file
        my_clone.data = self.data
        my_clone.date_time = self.date_time
        my_clone.graphs_file = self.graphs_file
        my_clone.html_template = self.html_template
        if system in self.logs:
            my_clone.logs[system] = self.logs[system]
        my_clone.reports_written = self.reports_written
        my_clone.reports_folder = self.reports_folder
        my_clone.store_folder = self.store_folder
        my_clone.system = system
        my_clone.threaded = self.threaded
        my_clone.year = self.year

        return my_clone

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
        for option in ['store_folder',
                       'reports_folder',
                       'graphs_definition_file',
                       'html_template',
                       'calculations_file']:
            if conf.has_option('MISC', option):
                self.__setattr__(
                    option,
                    self.get_absolute_path(conf.get('MISC', option))
                )

    def check_files(self):
        """
        Runtime test that checks if all required files exist and are readable:

        - settings file
        - calculations file (settings/MISC/calculations_file)
        - Jinja template (settings/MISC/html_template)
        - graphs definition file (settings/MISC/graphs_definition_file)
        - reports output folder (container.reports_folder)
        - CSV and DAT store folder (container.store_folder)

        Returns: Boolean (whether or not everything is in place)
        """
        if not os.path.isfile(self.settings_file):
            self.logger.error('Settings file not found: %s',
                              self.settings_file)
            raise collector.ConfigReadError
        try:
            self.get_external_files_from_config()
        except (collector.ConfigReadError,
                collector.ConfigParser.Error) as _exc:
            self.logger.error(repr(_exc))
            raise collector.ConfigReadError

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

        for option in ['graphs_definition_file',
                       'html_template',
                       'calculations_file']:
            try:
                self_attribute = self.__getattribute__(option)
            except AttributeError:
                raise collector.ConfigReadError
            self.logger.info('Cheking %s: %s', option, self_attribute)
            if not self_attribute or not os.path.isfile(self_attribute):
                self.logger.error('%s NOT found: %s', option, self_attribute)
                raise collector.ConfigReadError

    def date_tag(self):
        """ Converts self.date_time to the format '%Y%m%d_%H%M' """
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        return dt.date.strftime(current_date,
                                "%Y%m%d_%H%M")

    def create_report(self, system=None, container=None):
        """ Method for creating a single report for a particular system """
        report_name = '{0}/Report_{1}_{2}.html'.format(self.reports_folder,
                                                       self.date_tag(),
                                                       system or self.system)
        self.logger.debug('%s | Generating HTML report (%s)',
                          system or self.system,
                          report_name)
        with open(report_name, 'w') as output:
            output.writelines(gen_report(container=container or self))
        self.reports_written.append(report_name)

    def reports_generator(self):
        """
        Call jinja2 template, separately to safely store the logs
        in case of error.
        Doing this with threads throws many errors with Qt (when acting as
        matplotlib backend out from main thread)
        """
        if self.threaded:
            threads = [
                threading.Thread(target=self.create_report,
                                 kwargs={'system': system,
# TODO: Probar poniendo container = self
                                         'container': self.clone(system)},
                                 name=system)
                for system in self.data.system
            ]
            for thread_item in threads:
                thread_item.daemon = True
                thread_item.start()
            for thread_item in threads:  # Assuming all take the same time
                thread_item.join()
        else:
            for system in self.data.system:
                # self.system = system
                self.create_report(system)

    def local_store(self, nologs):
        """
        Makes a local copy of the current data in CSV and gzipped pickle
        """
        self.logger.info('Making a local copy of data in store folder: ')
        datetag = self.date_tag()
        destfile = '{0}/data_{1}.pkl'.format(self.store_folder, datetag)
        self.data.to_pickle(destfile,
                            compress=True)
        self.logger.info('  -->  %s.gz', destfile)
        destfile = '{0}/data_{1}.csv'.format(self.store_folder, datetag)
        self.data.to_csv(destfile)
        self.logger.info('  -->  %s', destfile)

        # Write logs
        if not nologs:
            for system in self.data.system:
                if system not in self.logs:
                    self.logger.warning('No log info found for %s', system)
                    continue
                with open('{0}/logs_{1}_{2}.txt'.format(self.store_folder,
                                                        system,
                                                        datetag),
                          'w') as logtxt:
                    logtxt.writelines(self.logs[system])

    def start(self,
              alldays=False,
              nologs=False,
              noreports=False,
              threads=False,
              **kwargs):
        """ Main method, gets data and logs, store and render the HTML output
            Threaded version (fast, error prone)
        """

        pylab.rcParams['figure.figsize'] = 13, 10
        plt.style.use('ggplot')

        # Open the tunnels and gather all data
        (self.data, self.logs) = collector.start(
            alldays=alldays,
            nologs=nologs,
            logger=self.logger,
            threads=threads,
            settings_file=self.settings_file
        )
        if self.data.empty:
            self.logger.error('Could not retrieve data!!! Aborting.')
            return

        # Store the data locally
        self.local_store(nologs)

        # Generate reports
        if noreports:
            self.logger.info('Skipped report generation')
        else:
            self.reports_generator()

        self.logger.info('Done!')
