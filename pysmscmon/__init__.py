#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 SMSCMon: T4-compliant CSV processor and visualizer for Acision SMSC Monitor
 ------------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 t4 input_file

 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information.

  line1: Collection date  (optional line)

  line2: Start time       (optional line)

  line3: Parameter Headings (comma separated).

 or

  ** Format 2: **

 line0: Header information containing T4 revision info and system information.
 line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple $
 line2: parameter headings (comma separated)
 ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple $

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing hash
  and contents followed by it (sometimes even more samples...) is ignored.

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
from . import logger

__version_info__ = (0, 8)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'

__all__ = ('main',
           'dump_config',
           'Container')

# Default figure size
pylab.rcParams['figure.figsize'] = 13, 10


def get_absolute_path(filename='', settings_file=None):
    """ Returns the absolute path if relative to the settings file location """
    if not settings_file:
        settings_file = smscmon.DEFAULT_SETTINGS_FILE
    relpath = os.path.dirname(os.path.abspath(settings_file))
    if not os.path.isabs(filename):
        return '{}{}{}'.format(relpath,
                               os.sep if relpath != os.sep else '',
                               filename)
    else:
        return filename


class Container(object):
    """ Object passed to jinja2 containg

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """

    def __init__(self, loglevel=None):
        if loglevel != 'keep':  # to use an existing logger, or during clone
            self.logger = logger.init_logger(loglevel)
        else:
            self.logger = None
        self.graphs = {}  # will be filled by calls from within jinja for loop
        self.logs = {}
        self.threaded = None
        self.year = dt.date.today().year
        self.date_time = dt.date.strftime(dt.datetime.today(),
                                          "%d/%m/%Y %H:%M:%S")
        self.data = smscmon.pd.DataFrame()
        self.system = ''
        self.html_template = ''
        self.graphs_file = ''
        self.store_folder = './store'
        self.reports_folder = './reports'

    def __str__(self):
        return 'Container created on {0} for system: {1}\nLoglevel: {2}\n' \
               'graphs_file: {3}\nhtml_template: {4}\nreports folder: {5}\n' \
               'store folder: {6}\ndata size: {7}'.format(self.year,
                                                          self.system,
                                                          self.logger.level,
                                                          self.graphs_file,
                                                          self.html_template,
                                                          self.reports_folder,
                                                          self.store_folder,
                                                          self.data.shape)

    def clone(self, system=''):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
        """
        my_clone = Container(loglevel='keep')
        my_clone.date_time = self.date_time
        my_clone.data = self.data
        if system in self.logs:
            my_clone.logs[system] = self.logs[system]
        my_clone.threaded = self.threaded
        my_clone.system = system
        my_clone.logger = self.logger
        my_clone.html_template = self.html_template
        my_clone.graphs_file = self.graphs_file

        return my_clone

    def th_reports(self, system):
        """  Handler for rendering jinja2 reports with threads.
             Called from threaded_main()
        """
        # Specify which system in container, passed to get_html_output
        container_clone = self.clone(system)
        self.logger.debug('%s | Generating HTML report', system)
        with open('{1}/Report_{0}_{2}.html'.
                  format(dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M"),
                         self.reports_folder, system), 'w') as output:
            output.writelines(gen_report(container=container_clone))

    def check_files(self, settings_file=None):
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
        if not settings_file:
            settings_file = smscmon.DEFAULT_SETTINGS_FILE

        if not os.path.isfile(settings_file):
            self.logger.error('Settings file not found: %s',
                              settings_file)
            return False
        if not os.path.exists(self.store_folder):
            self.logger.info('Creating non-existing directory: %s',
                             os.path.abspath(self.store_folder))
            os.makedirs(self.store_folder)
        if not os.path.exists(self.reports_folder):
            self.logger.info('Creating non-existing directory: %s',
                             os.path.abspath(self.reports_folder))
            os.makedirs(self.reports_folder)

        try:
            conf = smscmon.read_config(settings_file)
            if conf.has_option('MISC', 'store_folder'):
                self.store_folder = get_absolute_path(conf.get('MISC',
                                                               'store_folder'))
            if conf.has_option('MISC', 'reports_folder'):
                self.store_folder = get_absolute_path(conf.get('MISC',
                                                               'reports_folder'
                                                               ))
            calc_file = get_absolute_path(conf.get('MISC', 'calculations_file'),
                                          settings_file)
            self.graphs_file = get_absolute_path(
                conf.get('MISC', 'graphs_definition_file'),
                settings_file
                                                 )
            self.html_template = get_absolute_path(
                conf.get('MISC', 'html_template'),
                settings_file
                                                   )
        except (smscmon.ConfigReadError, smscmon.ConfigParser.Error) as _exc:
            self.logger.error(repr(_exc))
            return False

        if not calc_file or not os.path.isfile(calc_file):
            self.logger.error('Calculations file not found: %s', calc_file)
            return False

        if not self.html_template or not os.path.isfile(self.html_template):
            self.logger.error('HTML template not found: %s',
                              self.html_template)
            return False

        if not self.graphs_file or not os.path.isfile(self.graphs_file):
            self.logger.error('Graphs definitions file not found: %s',
                              self.graphs_file)
            return False

        return True


def main(alldays=False, nologs=False, noreports=False, threaded=False,
         **kwargs):
    """ Main method, gets data and logs, store and render the HTML output
        Threaded version (fast, error prone)
    """
    container = Container(loglevel=kwargs.pop('loglevel'))
    container.threaded = threaded
    # check everything's in place before doing anything
    if not container.check_files(kwargs.get('settings_file')):
        return

    pylab.rcParams['figure.figsize'] = 13, 10
    plt.style.use('ggplot')
    conf = smscmon.read_config(kwargs.get('settings_file'))

    # Open the tunnels and gather all data
    container.data, container.logs = smscmon.main(alldays=alldays,
                                                  nologs=nologs,
                                                  logger=container.logger,
                                                  threads=threaded,
                                                  settings_file=kwargs.get(
                                                      'settings_file'))
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
        generate_reports(all_systems, threaded)
    container.logger.info('Done!')


def generate_reports(container, all_systems):
    """
    Call jinja2 template, separately to safely store the logs
    in case of error.
    Doing this with threads throws many errors with Qt (when acting as
    matplotlib backend out from main thread)
    """
    if container.threaded:
        threads = [threading.Thread(target=container.th_reports,
                                    args=(system, ),
                                    name=system) for system in all_systems]
        for thread_item in threads:
            thread_item.daemon = True
            thread_item.start()
        for thread_item in threads:
            thread_item.join()
    else:
        for system in all_systems:
            container.system = system
            with open('{1}/Report_{0}_{2}.html'.
                      format(dt.date.strftime(dt.datetime.today(),
                                              "%Y%m%d_%H%M"),
                             container.reports_folder, container.system),
                      'w') as output:
                output.writelines(gen_report(container=container))


def dump_config(output=None):
    """ Dump current configuration to screen, useful for creating a new
    settings.cfg file """
    conf = smscmon.read_config()
    conf.write(output or sys.stdout)


def argument_parse():
    """ Argument parser for main method
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.
                                     RawTextHelpFormatter,
                                     description='SMSC Monitoring script (run '
                                                 'smscmon-config to dump the '
                                                 'configuration defaults)')
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
                        .format(os.path.relpath(smscmon.DEFAULT_SETTINGS_FILE))
                        )
    parser.add_argument('--loglevel', const=logger.DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        logger.DEFAULT_LOGLEVEL,
                        nargs='?')
    userargs = vars(parser.parse_args())

    # Default for smscmon is 'settings.cfg' in /conf
    if len(sys.argv) == 1:
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
    main(**userargs)


if __name__ == "__main__":
    argument_parse()
