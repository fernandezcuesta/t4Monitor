#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 SMSCMon: T4-compliant CSV processor and visualizer for Acision SMSC Monitor
 -----------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 t4 input_file

 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information

  line1: Collection date   (optional line)

  line2: Start time        (optional line)

  line3: Parameter Heading (comma separated)

 or

  ** Format 2: **

 line0: Header information containing T4 revision info and system information
 line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple `$`
 line2: parameter headings (comma separated)
 ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple `$`

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing
  hash and contents followed by it (sometimes even more samples...) is ignored

"""

from __future__ import absolute_import, print_function
import sys
import datetime as dt
import argparse
import threading
# Set matplotlib's backend before first import of pyplot or pylab,
# Qt4 doesn't like threads
import os
import matplotlib
if os.name == 'posix':
    matplotlib.use('Cairo')
else:
    matplotlib.use('TkAgg')
# Required by matplotlib when using TkAgg backend
#    import FileDialog

from matplotlib import pylab as pylab, pyplot as plt

from . import smscmon
from .gen_report import gen_report
from .logger import init_logger

__version_info__ = (0, 8, 3)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'

__all__ = ('main',
           'dump_config',
           'Container')

# Default figure size
pylab.rcParams['figure.figsize'] = 13, 10


class Container(object):
    """ Object passed to jinja2 containing:

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """

    def __init__(self, logger=None, loglevel=None, settings_file=None):
        self.graphs = {}  # will be filled by calls from within jinja for loop
        self.data = smscmon.pd.DataFrame()
        self.date_time = dt.date.strftime(dt.datetime.today(),
                                          "%d/%m/%Y %H:%M:%S")
        self.graphs_file = ''
        self.html_template = ''
        self.logger = logger
        if loglevel:
            self.logger = init_logger(loglevel)
        self.logs = {}
        self.reports_folder = './reports'
        self.settings_file = settings_file or smscmon.DEFAULT_SETTINGS_FILE
        self.store_folder = './store'
        self.system = ''
        self.threaded = None
        self.year = dt.date.today().year
        self.check_files()

    def __str__(self):
        return 'Container created on {0} for system: {1}\nLoglevel: {2}\n' \
               'graphs_file: {3}\nhtml_template: {4}\nreports folder: {5}\n' \
               'store folder: {6}\ndata size: {7}\nsettings_file: {8}'.format(
                self.year,
                self.system,
                self.logger.level,
                self.graphs_file,
                self.html_template,
                self.reports_folder,
                self.store_folder,
                self.data.shape,
                self.settings_file
                )

    def clone(self, system=''):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
        """
        my_clone = Container(logger=self.logger)
        my_clone.data = self.data
        my_clone.date_time = self.date_time
        my_clone.graphs_file = self.graphs_file
        my_clone.html_template = self.html_template
        if system in self.logs:
            my_clone.logs[system] = self.logs[system]
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

    def th_reports(self, system):
        """  Handler for rendering jinja2 reports with threads.
             Called from threaded_main()
        """
        # Specify which system in container, passed to get_html_output
        self.gen_system_report(system=system,
                               container=self.clone(system))

    def gen_system_report(self, system=None, container=None):
        report_name = '{0}/Report_{1}_{2}.html'.format(
            self.reports_folder,
            dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M"),
            system or self.system)
        self.logger.debug('%s | Generating HTML report (%s)',
                          system or self.system,
                          report_name)
        with open(report_name, 'w') as output:
            output.writelines(gen_report(container=container or self))

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
            raise smscmon.ConfigReadError
        try:
            conf = smscmon.read_config(self.settings_file)
            self.logger.debug('Using settings file: %s', self.settings_file)
            if conf.has_option('MISC', 'store_folder'):
                self.store_folder = self.get_absolute_path(
                    conf.get('MISC', 'store_folder')
                )
            self.logger.debug('Using store folder: %s', self.store_folder)

            if conf.has_option('MISC', 'reports_folder'):
                self.reports_folder = self.get_absolute_path(
                    conf.get('MISC', 'reports_folder')
                )
            self.logger.debug('Using reports folder: %s', self.reports_folder)

            calc_file = self.get_absolute_path(
                conf.get('MISC', 'calculations_file')
            )
            self.graphs_file = self.get_absolute_path(
                conf.get('MISC', 'graphs_definition_file')
                                                 )
            self.html_template = self.get_absolute_path(
                conf.get('MISC', 'html_template')
                                                   )
        except (smscmon.ConfigReadError, smscmon.ConfigParser.Error) as _exc:
            self.logger.error(repr(_exc))
            raise smscmon.ConfigReadError

        # Create store folder if needed
        if not os.path.exists(self.store_folder):
            self.logger.info('Creating non-existing directory: %s',
                             os.path.abspath(self.store_folder))
            os.makedirs(self.store_folder)

        # Create reports folder if needed
        if not os.path.exists(self.reports_folder):
            self.logger.info('Creating non-existing directory: %s',
                             os.path.abspath(self.reports_folder))
            os.makedirs(self.reports_folder)

        if not calc_file or not os.path.isfile(calc_file):
            self.logger.error('Calculations file not found: %s', calc_file)
            raise smscmon.ConfigReadError

        if not self.html_template or not os.path.isfile(self.html_template):
            self.logger.error('HTML template not found: %s',
                              self.html_template)
            raise smscmon.ConfigReadError

        if not self.graphs_file or not os.path.isfile(self.graphs_file):
            self.logger.error('Graphs definitions file not found: %s',
                              self.graphs_file)
            raise smscmon.ConfigReadError

    def generate_reports(self, all_systems):
        """
        Call jinja2 template, separately to safely store the logs
        in case of error.
        Doing this with threads throws many errors with Qt (when acting as
        matplotlib backend out from main thread)
        """
        if self.threaded:
            threads = [
                threading.Thread(target=self.th_reports,
                                 args=(system, ),
                                 name=system)
                for system in all_systems
            ]
            for thread_item in threads:
                thread_item.daemon = True
                thread_item.start()
            # for thread_item in threads:
                thread_item.join()
        else:
            for system in all_systems:
                # self.system = system
                self.gen_system_report(system)


def start(alldays=False, nologs=False, noreports=False, threaded=False,
          **kwargs):
    """ Main method, gets data and logs, store and render the HTML output
        Threaded version (fast, error prone)
    """
    try:
        container = Container(loglevel=kwargs.pop('loglevel')
                              if 'loglevel' in kwargs else None,
                              settings_file=kwargs.pop('settings_file')
                              if 'settings_file' in kwargs else None)
        container.threaded = threaded
    except smscmon.ConfigReadError:  # if not everything in place, return now
        return

    pylab.rcParams['figure.figsize'] = 13, 10
    plt.style.use('ggplot')
    conf = smscmon.read_config(container.settings_file)

    # Open the tunnels and gather all data
    container.data, container.logs = smscmon.main(
        alldays=alldays,
        nologs=nologs,
        logger=container.logger,
        threads=threaded,
        settings_file=container.settings_file
    )
    if container.data.empty:
        container.logger.error('Could not retrieve data!!! Aborting.')
        return

    all_systems = [x for x in conf.sections() if x not in ['GATEWAY', 'MISC']]
    datetag = dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M")

    # Store the data locally
    container.logger.info('Making a local copy of data in store folder: ')
    destfile = '{0}/data_{1}.pkl'.format(container.store_folder, datetag)
    container.data.to_pickle(destfile,
                             compress=True)
    container.logger.info('  -->  %s.gz', destfile)
    destfile = '{0}/data_{1}.csv'.format(container.store_folder, datetag)
    container.data.to_csv(destfile)
    container.logger.info('  -->  %s', destfile)

    # Write logs
    if not nologs:
        for system in all_systems:
            if system not in container.logs:
                container.logger.warning('No log info found for %s', system)
                continue
            with open('{0}/logs_{1}_{2}.txt'.format(container.store_folder,
                                                    system,
                                                    datetag),
                      'w') as logtxt:
                logtxt.writelines(container.logs[system])
    # Generate reports
    if noreports:
        container.logger.info('Skipped report generation')
    else:
        container.generate_reports(all_systems)
    container.logger.info('Done!')


def dump_config(output=None):
    """ Dump current configuration to screen, useful for creating a new
    settings.cfg file """
    conf = smscmon.read_config()
    conf.write(output or sys.stdout)


def argument_parse(args=None):
    """ Argument parser for main method
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.
                                     RawTextHelpFormatter,
                                     description='SMSC Monitoring script (run'
                                                 ' smscmon-config to dump the'
                                                 ' configuration defaults)')
    parser.add_argument('--all', action='store_true', dest='alldays',
                        help='Collect all data available on SMSCs'
                             'not just for today')
    parser.add_argument('--fast', action='store_true', dest='threaded',
                        help='Fast mode running with threads, '
                             'lowering execution time by 1/2.')
    parser.add_argument('--noreports', action='store_true',
                        help='Skip report creation, files are just gathered '
                             'and stored locally')
    parser.add_argument('--nologs', action='store_true',
                        help='Skip log information collection from SMSCs')
    parser.add_argument('--settings', dest='settings_file',
                        default=smscmon.DEFAULT_SETTINGS_FILE,
                        help='Settings file (default {})'
                        .format(os.path.relpath(smscmon.DEFAULT_SETTINGS_FILE)
                                ))
    parser.add_argument('--loglevel', const=logger.DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        logger.DEFAULT_LOGLEVEL,
                        nargs='?')
    # Default for smscmon is 'settings.cfg' in /conf
    if not args:
        parser.print_help()
        print('')
        while True:
            ans = raw_input('No arguments were specified, continue with '
                            'defaults (check with smscmon-config)? ([Y]|n) ')
            if not ans or ans in ('y', 'Y'):
                print('')
                break
            elif ans in ('n', 'N'):
                sys.exit('Aborting')
            print('Please enter y or n.')
    return vars(parser.parse_args(args))


def main():
    start(**argument_parse(sys.argv[1:]))


if __name__ == "__main__":
    main()
