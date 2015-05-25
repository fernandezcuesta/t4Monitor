#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Main methods for pysmscmon
"""

import sys
import datetime as dt
import argparse
import pandas as pd
import threading
import ConfigParser
from pysmscmon import smscmon
from pysmscmon.gen_report import gen_report
from os import path, makedirs
from matplotlib import pylab as pylab, pyplot as plt

__version_info__ = (0, 6, 3)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'


pylab.rcParams['figure.figsize'] = 13, 10



class Container(object):
    """ Object passed to jinja2 containg

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """
    graphs = {}  # will be filled by calls from within jinja for loop
    logs = {}
    year = dt.date.today().year
    date_time = dt.date.strftime(dt.datetime.today(), "%d/%m/%Y %H:%M:%S")
    data = pd.DataFrame()
    system = ''
    logger = None
    html_template = ''
    graphs_file = ''
    store_folder = './store'
    reports_folder = './reports'

    def __init__(self, loglevel=None):
        if loglevel != 'keep':
            self.logger = smscmon.init_logger(loglevel)

    def __str__(self):
        return 'Container created on {} for system: {}\nLoglevel: {}\n' + \
               'graphs_file: {}\nhtml_template: {}\ndata size: {}\n' + \
               'store folder: {}\nreports folder: {}'.format(self.year,
                                                             self.system,
                                                             self.logger.level,
                                                             self.graphs_file,
                                                             self.html_template,
                                                             self.data.shape,
                                                             self.store_folder,
                                                             self.reports_folder
                                                             )

    def clone(self, system):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
        """
        my_clone = Container(loglevel='keep')
        my_clone.date_time = self.date_time
        my_clone.data = self.data
        if system in self.logs:
            my_clone.logs = {system: self.logs[system]}
        my_clone.system = system
        my_clone.logger = self.logger
        my_clone.html_template = self.html_template
        my_clone.graphs_file = self.graphs_file

        return my_clone


def th_reports(container, system):
    """  Handler for rendering jinja2 reports with threads.
         Called from threaded_main()
    """
    # Specify which system in container, passed to get_html_output for render
    container_clone = container.clone(system)
    container.logger.debug('%s| Generating HTML report', system)
    with open('{1}/Report_{0}_{2}.html'.
              format(dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M"),
                     container.reports_folder, system), 'w') as output:
        output.writelines(gen_report(container=container_clone))


def main(alldays=False, nologs=False, noreports=False, threaded=False,
         **kwargs):
    """ Main method, gets data and logs, store and render the HTML output
        Threaded version (fast, error prone)
    """
    container = Container(loglevel=kwargs.get('loglevel'))

    if not check_files(container):  # check everything's in place
        return

    pylab.rcParams['figure.figsize'] = 13, 10
    plt.style.use('ggplot')
    conf = smscmon.read_config()

    # Open the tunnels and gather all data
    container.data, container.logs = smscmon.main(alldays=alldays,
                                                  nologs=nologs,
                                                  logger=container.logger,
                                                  threads=threaded)
    if container.data.empty:
        container.logger.error('Could not retrieve data!!! Aborting.')
        return

    all_systems = [x for x in conf.sections() if x not in ['GATEWAY', 'MISC']]
    datetag = dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M")

    # Store the data locally
    container.logger.info('Making a local copy of data in store folder')

    container.data.to_pickle('{0}/data_{1}.pkl'.format(container.store_folder,
                                                       datetag),
                             compress=True)
    container.data.to_csv('{0}/data_{1}.csv'.format(container.store_folder,
                                                    datetag))

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
    if not noreports:
        # Call jinja2 template, separately to safely store the logs
        # in case of error.
        # Doing this with threads throws many errors with Qt (when acting as
        # matplotlib backend out from main thread)
        if threaded:
            threads = [threading.Thread(target=th_reports,
                                        args=(container, system),
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
    else:
        container.logger.info('Skipped report generation')
    container.logger.info('Done!')


def check_files(container):
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

    if not path.isfile(smscmon.SETTINGS_FILE):
        container.logger.error('Settings file not found: %s',
                               smscmon.SETTINGS_FILE)
        return False

    relpath = path.dirname(path.abspath(smscmon.SETTINGS_FILE))

    if not path.exists(container.store_folder):
        container.logger.info('Creating non-existing directory: %s',
                              path.abspath(container.store_folder))
        makedirs(container.store_folder)
    if not path.exists(container.reports_folder):
        container.logger.info('Creating non-existing directory: %s',
                              path.abspath(container.reports_folder))
        makedirs(container.reports_folder)

    try:
        conf = smscmon.read_config(smscmon.SETTINGS_FILE)
        if conf.has_option('MISC', 'store_folder'):
            container.store_folder = '{}/{}'.format(relpath,
                                                    conf.get('MISC',
                                                             'store_folder'))
        if conf.has_option('MISC', 'reports_folder'):
            container.store_folder = '{}/{}'.format(relpath,
                                                    conf.get('MISC',
                                                             'reports_folder'))
        calc_file = conf.get('MISC', 'calculations_file')
        graphs_file = conf.get('MISC', 'graphs_definition_file')
        html_template = conf.get('MISC', 'html_template')

        if not path.isabs(calc_file):
            calc_file = '{}/{}'.format(relpath, calc_file)
        if not path.isabs(graphs_file):
            container.graphs_file = '{}/{}'.format(relpath, graphs_file)
        if not path.isabs(html_template):
            container.html_template = '{}/{}'.format(relpath, html_template)

    except (smscmon.ConfigReadError, ConfigParser.Error) as _exc:
        container.logger.error(repr(_exc))
        return False

    if not calc_file or not path.isfile(calc_file):
        container.logger.error('Calculation file not found: %s', calc_file)
        return False

    if not container.html_template or not path.isfile(container.html_template):
        container.logger.error('HTML template not found: %s',
                               container.html_template)
        return False

    if not container.graphs_file or not path.isfile(container.graphs_file):
        container.logger.error('Graph definitions file not found: %s',
                               container.graphs_file)
        return False

    return True



def dump_config():
    """ Dump current configuration to screen, useful for creating a new
    settings.cfg file """
    conf = smscmon.read_config()
    conf.write(sys.stdout)


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
    parser.add_argument('--settings', default=smscmon.SETTINGS_FILE,
                        help='Settings file (default {})'.format(path.relpath( \
                        smscmon.SETTINGS_FILE)))
    parser.add_argument('--loglevel', const=smscmon.DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        smscmon.DEFAULT_LOGLEVEL,
                        nargs='?')
    userargs = vars(parser.parse_args())

    smscmon.SETTINGS_FILE = userargs.pop('settings')
    # Default for smscmon is 'settings.cfg' in /conf
    if len(sys.argv) == 1:
        parser.print_help()
        print ''
        while True:
            ans = raw_input('No arguments were specified, continue with '
                            'defaults (check with smscmon-config)? (y|n) ')
            if ans not in ['y', 'Y', 'n', 'N']:
                print 'Please enter y or n.'
                continue
            if not ans or ans == 'y' or ans == 'Y':
                print ''
                break
            if ans == 'n' or ans == 'N':
                sys.exit('Aborting')
    main(**userargs)


if __name__ == "__main__":
    argument_parse()
