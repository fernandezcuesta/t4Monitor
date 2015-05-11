#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Report generator module based on Jinja2
"""

import smscmon
import os
import jinja2
import datetime as dt
import argparse
import pandas as pd
import threading
import ConfigParser
#from ast import literal_eval
from matplotlib import pylab as pylab, pyplot as plt


__version_info__ = (0, 6, 2)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_TEMPLATE = 'template.html'
GRAPHS_DEFINITION_FILE = 'graphs_list.cnf'
OUTPUT_FOLDER = 'reports'
STORE_FOLDER = 'store'

pylab.rcParams['figure.figsize'] = 13, 10


def get_html_output(container):
    """ Create the jinja2 environment.
    Notice the use of trim_blocks, which greatly helps control whitespace. """

    try:
        assert container.system != ''  # Check a system was specified
        assert not container.data.empty  # Check that data isn't empty

        j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(THIS_DIR),
                                    trim_blocks=True)
        j2_tpl = j2_env.get_template(container.html_template)
        j2_tpl.globals['get_graphs'] = get_graphs
        container.logger.info('%s| Generating graphics and rendering report',
                              container.system)
        return j2_tpl.render(data=container)
    except AssertionError:
        container.logger.error('Data container error!')
        return ['']
    except Exception as other_exception:
        container.logger.error('%s| Unexpected exception found while '
                               'rendering report: %s',
                               container.system,
                               repr(other_exception))
        return ['']


def get_graphs(container):
    """ Produces b64 encoded graphs for the selected system.
        data (pandas.DataFrame) is used implicitly while evaluating the
        command (_cmd) received from draw_command()

        Returns: (graph_title, graph_encoded_in_b64)
    """

    with open(GRAPHS_DEFINITION_FILE, 'r') as graphs_txt:
        for line in graphs_txt:
            line = line.strip()
            if not len(line) or line[0] == '#':
                continue
            info = line.split(';')
            if len(info) == 1:
                container.logger.warning('Bad format in current line: "%s"',
                                         line)
                continue
            try:
                optional_kwargs = eval("dict(%s)" % info[2]) \
                                  if len(info) == 3 else {'ylim': 0.0}
            except ValueError:
                optional_kwargs = {'ylim': 0.0}

            container.logger.debug('%s|  Plotting %s',
                                   container.system,
                                   info[0])
            try:
                _b64figure = smscmon.to_base64(getattr(smscmon,
                                                       "plot_var")\
                    (container.data,
                     *[x.strip() for x in info[0].split(',')],
                     system=container.system.upper(),
                     logger=container.logger,
                     **optional_kwargs))
                if not _b64figure:
                    yield False
                else:
                    yield (info[1], _b64figure)
            except Exception as exc:
                container.logger.error('Unexpected error while '
                                       'rendering graph: %s', repr(exc))
                yield False


def th_reports(container, system):
    """  Handler for rendering jinja2 reports with threads.
         Called from threaded_main()
    """
    # Specify which system in container, passed to get_html_output for render
    container_clone = container.clone(system)
    container.logger.debug('%s| Generating HTML report', system)
    with open('{1}/Report_{0}_{2}.html'.
              format(dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M"),
                     OUTPUT_FOLDER, system), 'w') as output:
        output.writelines(get_html_output(container=container_clone))


def main(alldays=False, nologs=False, noreports=False, threaded=False,
         **kwargs):
    """ Main method, gets data and logs, store and render the HTML output
        Threaded version (fast, error prone)
    """
    pylab.rcParams['figure.figsize'] = 13, 10
    plt.style.use('ggplot')
    container = Container()
    container.logger = smscmon.init_logger(kwargs.get('loglevel'))
    
    container.html_template = kwargs.get('html_template', HTML_TEMPLATE)
    
    if not check_files(container.logger):  # check everything's in place
        return

    # Open the tunnels and gather all data
    container.data, container.logs = smscmon.main(alldays,
                                                  nologs,
                                                  container.logger,
                                                  threaded)
    if container.data.empty:
        container.logger.error('Could not retrieve data!!! Aborting.')
        return

    conf = smscmon.read_config()

    all_systems = [x for x in conf.sections() if x not in ['GATEWAY', 'MISC']]
    datetag = dt.date.strftime(dt.datetime.today(), "%Y%m%d_%H%M")

    # Store the data locally
    container.logger.info('Making a local copy of data in store folder')

    container.data.to_pickle('{0}/data_{1}.pkl'.format(STORE_FOLDER, datetag),
                             compress=True)
    container.data.to_csv('{0}/data_{1}.csv'.format(STORE_FOLDER, datetag))

    # Write logs
    if not nologs:
        for system in all_systems:
            if system not in container.logs:
                container.logger.warning('No log info found for %s', system)
                continue
            with open('{0}/logs_{1}_{2}.txt'.format(STORE_FOLDER,
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
                                 OUTPUT_FOLDER, container.system),
                          'w') as output:
                    output.writelines(get_html_output(container=container))
    else:
        container.logger.info('Skipped report generation')
    container.logger.info('Done!')


def check_files(logger):
    """
    Runtime test that checks if all required files exist and are readable:

    - settings file (pysmscmon.SETTINGS_FILE)
    - calculations file (settings/MISC/calculations_file)
    - Jinja template (HTML_TEMPLATE)
    - graphs definition file (GRAPHS_DEFINITION_FILE)
    - reports output folder (OUTPUT_FOLDER)
    - CSV and DAT store folder (STORE_FOLDER)

    Returns: Boolean (whether or not everything is in place)
    """

    # Check output subfolders exist
    if not os.path.exists(STORE_FOLDER):
        logger.info('Creating non-existing subfolder /%s', STORE_FOLDER)
        os.makedirs(STORE_FOLDER)
    if not os.path.exists(OUTPUT_FOLDER):
        logger.info('Creating non-existing subfolder /%s', OUTPUT_FOLDER)
        os.makedirs(OUTPUT_FOLDER)

    if not os.path.isfile(smscmon.SETTINGS_FILE):
        logger.error('Settings file not found: %s', smscmon.SETTINGS_FILE)
        return False

    try:
        conf = smscmon.read_config(smscmon.SETTINGS_FILE)
        calc_file = conf.get('MISC', 'calculations_file')
    except (smscmon.ConfigReadError, ConfigParser.Error) as _exc:
        logger.error(repr(_exc))
        return False

    if not calc_file or not os.path.isfile(calc_file):
        logger.error('Calculation file not found: %s', calc_file)
        return False

    if not os.path.isfile(HTML_TEMPLATE):
        logger.error('HTML template not found: %s', HTML_TEMPLATE)
        return False

    if not os.path.isfile(GRAPHS_DEFINITION_FILE):
        logger.error('Graph definitions file not found: %s',
                     GRAPHS_DEFINITION_FILE)
        return False

    return True


class Container(object):
    """ Object passed to jinja2 containg

        - graphs: dictionary of key, value -> {system-id, list of graphs}
        - logs: dictionary of key, value -> {system-id, log entries}
        - date_time: Report generation date and time
        - data: dataframe passed to get_graphs()
        - system: string containing current system-id being rendered
    """
    graphs = {}  # will be filled by calls from within jinja for loop
    num_graphs = 0
    logs = {}
    year = dt.date.today().year
    date_time = dt.date.strftime(dt.datetime.today(), "%d/%m/%Y %H:%M:%S")
    data = pd.DataFrame()
    system = ''
    logger = None
    html_template = HTML_TEMPLATE

    def clone(self, system):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
        """
        my_clone = Container()
        my_clone.date_time = self.date_time
        my_clone.data = self.data
        # Backwards compatibility with non threaded version
        my_clone.logs = {system: self.logs[system]}
        my_clone.system = system
        my_clone.logger = self.logger
        my_clone.html_template = self.html_template

        return my_clone


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='SMSC Monitoring script',
                                     formatter_class=argparse.
                                     RawTextHelpFormatter)

    PARSER.add_argument('--all', action='store_true', dest='alldays',
                        help='Collect all data available on SMSCs'
                             'not just for today')
    PARSER.add_argument('--fast', action='store_true', dest='threaded',
                        help='Fast mode running with threads, '
                             'lowering execution time by 1/2.')
    PARSER.add_argument('--noreports', action='store_true',
                        help='Skip report creation, files are just gathered '
                             'and stored locally')
    PARSER.add_argument('--nologs', action='store_true',
                        help='Skip log information collection from SMSCs')
    PARSER.add_argument('--settings',
                        default=smscmon.SETTINGS_FILE,
                        help='Settings file (default: {})'.format(
                            smscmon.SETTINGS_FILE))
    PARSER.add_argument('--template', default=HTML_TEMPLATE,
                        help='HTML template (default: %s)' % HTML_TEMPLATE
                       )
    PARSER.add_argument('--loglevel', const=smscmon.DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        smscmon.DEFAULT_LOGLEVEL,
                        nargs='?')
    ARGS = vars(PARSER.parse_args())

    smscmon.SETTINGS_FILE = ARGS.pop('settings')
    # Default for pysmscmon is 'settings.cfg' in same folder as pysmscmon.py

    main(**ARGS)
