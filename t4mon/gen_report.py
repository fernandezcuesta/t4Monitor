#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Report generator module based on Jinja2
"""
from __future__ import absolute_import

import datetime as dt
from os import path
# from ast import literal_eval  # TODO: is literal_eval working in Linux?

import jinja2
import tqdm
from matplotlib import pyplot as plt

from . import gen_plot
from .logger import init_logger


class Report(object):

    """
    Generate an HTML report based from self.data, drawing all the items in
    self.graphs_definition_file.

    Class arguments:
    - container
        Type: t4mon.Orchestrator
        Description: Object containing the following mandatory fields:
                     + data:
                         pandas DataFrame
                         MultiIndex dataframe that will be used as data source
                     + graphs_definition_file: (*see format below)
                         string
                         path of the file where graphs to be drawn are defined
                     + html_template
                         string
                         path of the template passed to Jinja2
                     + logs
                         dict
                         log output (value) corresponding for each system (key)
                     + date_time
                         string
                         collection timestamp in the format '%d/%m/%Y %H:%M:%S'
    - system:
        Type: string
        Description: Identifier of the system for which the report will be
                     generated. It must be a valid identifier present in
                     container.data, more specifically matching one of
                     container.data.index.levels[-1]

    - logger [optional]
        Type: logging.Logger
        Default: None
        Description: logging object optionally passed from the container



    Graphs definition file format
    -----------------------------
        ######################################################################
        #                                                                    #
        # Syntax (all lines starting with hash will be treated as comments): #
        # var_names;title;plot_options                                       #
        #                                                                    #
        # Where:                                                             #
        # var_names:   list of partial variable names (* wildcard allowed)   #
        #              separated with commas                                 #
        # title:       string containing graph's title                       #
        # plot_option: [optional] comma-separated options passed             #
        #              transparently to matplotlib                           #
        ######################################################################

        # This is just a comment. No inline comments allowed.
        message_buffered;Test 1
        successful_FDA;Test 2 (percentage);ylim=(0.0,100.0),linewidth=2

    """

    def __init__(self, container, system, logger=None):
        self.system = system
        # Transparently pass all container items
        for item in container.__dict__:
            self.__setattr__(item, container.__getattribute__(item))
        if 'loglevel' not in self.__dict__:
            self.loglevel = logger.DEFAULT_LOGLEVEL
        if logger:
            self.logger = logger
        if 'logger' not in self.__dict__ or not self.logger:
            self.logger = init_logger(self.loglevel)
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        self.year = current_date.year

    def render(self):
        """
        Create the jinja2 environment.
        Notice the use of trim_blocks, which greatly helps control whitespace.
        """
        try:
            assert not self.data.empty  # Check that data isn't empty
            assert self.system  # Check a system was specified
            env_dir = path.dirname(path.abspath(self.html_template))
            j2_env = jinja2.Environment(
                     loader=jinja2.FileSystemLoader(env_dir),
                     trim_blocks=True
                     )
            j2_tpl = j2_env.get_template(
                     path.basename(self.html_template)
                     )
            j2_tpl.globals['render_graphs'] = self.render_graphs
            self.logger.info('%s | Generating graphics and rendering report '
                             '(may take a while)', self.system)
            return j2_tpl.generate(data=self)
        except AssertionError:
            self.logger.error(
                '%s',
                '{} | No data, no report'.format(self.system)
                if self.system else 'Not a valid system, report skipped'
            )
        except IOError:
            self.logger.error('Template file (%s) not found.',
                              self.html_template)
        except jinja2.TemplateError as msg:
            self.logger.error('%s | Error in html template (%s): %s',
                              self.system,
                              self.html_template,
                              repr(msg))
        # Stop the generator in case of exception
        raise StopIteration

    def render_graphs(self):
        """
        Produce b64 encoded graphs for the selected system.
        Return: (graph_title, graph_encoded_in_b64)
        """
        try:
            progressbar_prefix = 'Rendering report for {}'.format(self.system)
            with open(self.graphs_definition_file, 'r') as graphs_txt:
                graphs_txt_contents = graphs_txt.readlines()
            for line in tqdm.tqdm(graphs_txt_contents,
                                  leave=True,
                                  desc=progressbar_prefix,
                                  unit='Graphs'):
                line = line.strip()

                if not len(line) or line[0] == '#':
                    continue
                info = line.split(';')
                # info[0] contains a comma-separated list of parameters to
                # be drawn
                # info[1] contains the title
                # info[2] contains the plot options
                if len(info) == 1:
                    self.logger.warning('Bad format in current line: '
                                        '"%s"...', line[1:20])
                    continue
                try:
                    optional_kwargs = eval("dict(%s)" % info[2]) \
                                      if len(info) == 3 else {'ylim': 0.0}
                except ValueError:
                    optional_kwargs = {'ylim': 0.0}

                self.logger.debug('%s |  Plotting %s',
                                  self.system,
                                  info[0])
                # Generate figure and encode to base64
                plot_axis = gen_plot.plot_var(
                    self.data,
                    *[x.strip() for x in info[0].split(',')],
                    system=self.system,
                    logger=self.logger,
                    **optional_kwargs
                                               )
                _b64figure = gen_plot.to_base64(plot_axis)
                plt.close(plot_axis.get_figure())  # close figure
                if _b64figure:
                    yield (info[1].strip(), _b64figure)
        except IOError:
            self.logger.error('Graphs definition file not found: %s',
                              self.graphs_definition_file)
        except Exception as unexpected:
            self.logger.error('%s | Unexpected exception found while '
                              'creating graphs: %s',
                              self.system,
                              repr(unexpected))
        yield None


def gen_report(container, system):
    """
    Convenience function for calling Report.render() method
    """
    _report = Report(container, system)
    return _report.render()
