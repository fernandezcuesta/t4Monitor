#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Report generator module based on Jinja2
"""
from __future__ import absolute_import

from os import path
from ast import literal_eval

import jinja2
from matplotlib import pyplot as plt

from . import gen_plot


def gen_report(container=None, system=None):
    """ Create the jinja2 environment.
    Notice the use of trim_blocks, which greatly helps control whitespace. """

    try:
        assert not container.data.empty  # Check that data isn't empty
        assert system  # Check a system was specified
        container.system = system
        env_dir = path.dirname(path.abspath(container.html_template))
        j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(env_dir),
                                    trim_blocks=True)
        j2_tpl = j2_env.get_template(path.basename(container.html_template))
        j2_tpl.globals['get_graphs'] = get_graphs
        container.logger.info('%s | Generating graphics and rendering report',
                              system)
        return j2_tpl.render(data=container)
    except AssertionError:
        container.logger.error(
            '%s',
            '{} | No data, no report'.format(system)
            if system else 'Not a valid system, report skipped'
        )
    except IOError:
        container.logger.error('Template file (%s) not found.',
                               container.html_template)
    except jinja2.TemplateError as msg:
        container.logger.error('%s | Error in html template (%s): %s',
                               system,
                               container.html_template,
                               repr(msg))
    return ''


def get_graphs(container):
    """ Produces b64 encoded graphs for the selected system.
        data (pandas.DataFrame) is used implicitly while evaluating the
        command (_cmd) received from draw_command()

        Returns: (graph_title, graph_encoded_in_b64)
    """
    try:
        with open(container.graphs_definition_file, 'r') as graphs_txt:
            for line in graphs_txt:
                line = line.strip()

                if not len(line) or line[0] == '#':
                    continue
                info = line.split(';')
                if len(info) == 1:
                    container.logger.warning('Bad format in current line: '
                                             '"%s"...', line[1:20])
                    continue
                try:
                    optional_kwargs = literal_eval("dict(%s)" % info[2]) \
                                      if len(info) == 3 else {'ylim': 0.0}
                except ValueError:
                    optional_kwargs = {'ylim': 0.0}

                container.logger.debug('%s |  Plotting %s',
                                       container.system,
                                       info[0])
                # Generate figure and encode to base64
                plot_axis = gen_plot.plot_var(
                    container.data,
                    *[x.strip() for x in info[0].split(',')],
                    system=container.system.upper(),
                    logger=container.logger,
                    **optional_kwargs
                                               )
                _b64figure = gen_plot.to_base64(plot_axis)
                plt.close(plot_axis.get_figure())  # close figure
                if _b64figure:
                    yield (info[1].strip(), _b64figure)
    except IOError:
        container.logger.error('Graphs definition file not found: %s',
                               container.graphs_definition_file)
    except Exception as unexpected:
        container.logger.error('%s | Unexpected exception found while '
                               'creating graphs: %s',
                               container.system,
                               repr(unexpected))
    yield None
