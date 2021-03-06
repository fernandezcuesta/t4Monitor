#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Report generator module based on **Jinja2**
"""

import codecs
import datetime as dt
from os import path

import six

import tqdm
import jinja2
from t4mon import gen_plot, arguments
from matplotlib import pyplot as plt
from t4mon.logger import init_logger


# from ast import literal_eval  # TODO: is literal_eval working in Linux?


class Report(object):

    """Generate an HTML report, drawing all the items defined in a
    ``graphs_definition_file``.

    Arguments:
        container (t4mon.Orchestrator): cosa

        system (str):
            Identifier of the system for which the report will be
            generated.
            It must be a valid identifier present in ``container.data``,
            more specifically matching one of
            ``container.data.index.levels[-1]``.

        logger (Optional[logging.Logger]):
            logging object optionally passed directly

    Note:
        Attributes in container are passed transparently to Report

    Attributes:
        data (pandas.DataFrame):
            MultiIndex dataframe that will be used as data source
        settings_file (str):
            Settings filename where ``graphs_definition_file`` and
            ``html_template`` are defined
        logs (dict):
             log output (value) corresponding for each system (key)
        date_time (str):
             collection timestamp in the format ``%d/%m/%Y %H:%M:%S``

    Note:
        **Graphs definition file format** ::

            ###################################################################
            #                                                                 #
            # Syntax (all lines starting with # will be treated as comments): #
            # var_names;title;plot_options                                    #
            #                                                                 #
            # Where:                                                          #
            # var_names:   list of regular expressions matching column names  #
            #              separated with commas                              #
            # title:       string containing graph's title                    #
            # plot_option: [optional] comma-separated options passed          #
            #              transparently to matplotlib                        #
            ###################################################################

            # This is just a comment. No inline comments allowed.
            message_buffered;Test 1
            successful_FDA;Test 2 (percentage);ylim=(0.0,100.0),linewidth=2
    """

    def __init__(self, container, system, logger=None):
        self.system = system
        # Transparently pass all container items
        for item in container.__dict__:
            setattr(self, item, getattr(container, item))
        if 'loglevel' not in self.__dict__:
            self.loglevel = logger.DEFAULT_LOGLEVEL
        self.logger = logger or init_logger(self.loglevel)
        current_date = dt.datetime.strptime(self.date_time,
                                            "%d/%m/%Y %H:%M:%S")
        self.year = current_date.year
        # populate self.html_template and self.graphs_definition_file
        conf = arguments.read_config(self.settings_file)
        for item in ['html_template', 'graphs_definition_file']:
            setattr(self,
                    item,
                    arguments.get_absolute_path(conf.get('MISC', item),
                                                self.settings_file))

    def render(self):
        """
        Create the Jinja2 environment.
        Notice the use of `trim_blocks`, which greatly helps control
        whitespace.
        """
        try:
            assert not self.data.empty  # Check that data isn't empty
            assert self.system  # Check a system was specified
            env_dir = path.dirname(path.abspath(self.html_template))
            j2_env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(env_dir),
                trim_blocks=True
            )
            j2_tpl = j2_env.get_template(path.basename(self.html_template))
            j2_tpl.globals['render_graphs'] = self.render_graphs
            self.logger.info('{0} | Generating graphics and rendering report '
                             '(may take a while)'.format(self.system))
            return j2_tpl.generate(data=self)
        except AssertionError:
            self.logger.error('{0} | No data, no report'.format(self.system)
                              if self.system
                              else 'Not a valid system, report skipped')
        except IOError:
            self.logger.error('Template file ({0}) not found.'
                              .format(self.html_template))
        except jinja2.TemplateError as msg:
            self.logger.error('{0} | Error in html template ({1}): {2}'
                              .format(self.system,
                                      self.html_template,
                                      repr(msg)))
        # Stop the generator in case of exception
        raise StopIteration

    def render_graphs(self):
        """ Produce base64 encoded graphs for the selected system
        (``self.system``).

        Yield:
            tuple: (``graph_title``, ``graph_encoded_in_b64``)
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
                                        '"{0}"...'.format(line[1:20]))
                    continue
                try:
                    optional_kwargs = eval(
                        "dict({0})".format(info[2])
                    ) if len(info) == 3 else {'ylim': 0.0}
                except ValueError:
                    optional_kwargs = {'ylim': 0.0}

                self.logger.debug('{0} |  Plotting {1}'.format(self.system,
                                                               info[0]))
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
                    yield (six.u(info[1].strip()),
                           codecs.decode(_b64figure, 'utf-8'))
        except IOError:
            self.logger.error('Graphs definition file not found: {0}'
                              .format(self.graphs_definition_file))
        except Exception as unexpected:
            self.logger.error('{0} | Unexpected exception found while '
                              'creating graphs: {1}'.format(self.system,
                                                            repr(unexpected)))
        yield None


def gen_report(container, system):
    """
    Convenience function for calling :meth:`.Report.render()` method

    Arguments:
        container (t4mon.Orchestrator):
            object containing all the information required to render the report
        system (str):
            system for which the report will be generated
    Return:
        str
    """
    _report = Report(container, system)
    return _report.render()
