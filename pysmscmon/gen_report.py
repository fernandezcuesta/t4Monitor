#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Report generator module based on Jinja2
"""

import jinja2
from os import path
from pysmscmon import gen_plot
from ast import literal_eval

def gen_report(container):
    """ Create the jinja2 environment.
    Notice the use of trim_blocks, which greatly helps control whitespace. """

    try:
        assert container.system != ''  # Check a system was specified
        assert not container.data.empty  # Check that data isn't empty
        env_dir = path.dirname(path.abspath(container.html_template))
        j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(env_dir),
                                    trim_blocks=True)
        j2_tpl = j2_env.get_template(path.basename(container.html_template))
        j2_tpl.globals['get_graphs'] = get_graphs
        container.logger.info('%s| Generating graphics and rendering report',
                              container.system)
        return j2_tpl.render(data=container)
    except AssertionError:
        container.logger.error('Data container error!')
        return ['']
    except Exception as unexpected:
        container.logger.error('%s| Unexpected exception found while '
                               'rendering report: %s',
                               container.system,
                               repr(unexpected))
        return ['']


def get_graphs(container):
    """ Produces b64 encoded graphs for the selected system.
        data (pandas.DataFrame) is used implicitly while evaluating the
        command (_cmd) received from draw_command()

        Returns: (graph_title, graph_encoded_in_b64)
    """
    try:
        container.logger.debug(container.graphs_file)
        with open(container.graphs_file, 'r') as graphs_txt:
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
                    optional_kwargs = literal_eval("dict(%s)" % info[2]) \
                                      if len(info) == 3 else {'ylim': 0.0}
                except ValueError:
                    optional_kwargs = {'ylim': 0.0}
    
                container.logger.debug('%s|  Plotting %s',
                                       container.system,
                                       info[0])
                try:
                    _b64figure = gen_plot.to_base64(getattr(gen_plot,
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
    except Exception as unexpected:
        container.logger.error('%s| Unexpected exception found while '
                               'creating graphs: %s',
                               container.system,
                               repr(unexpected))
        yield None
