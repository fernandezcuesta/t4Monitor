#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# isort:skip_file

import os
import sys

import matplotlib  # isort:skip
# Set matplotlib's backend before first import of pyplot or pylab,
# Qt4 doesn't like threads
# TODO: unify matplotlib backends
if os.name == 'posix':
    matplotlib.use('Cairo')
else:
    matplotlib.use('TkAgg')
    import six.moves.tkinter_filedialog  # Required when using TkAgg backend

from t4mon._version import get_versions
from t4mon.collector import add_methods_to_pandas_dataframe, read_pickle
from t4mon.gen_plot import plot_var
from t4mon.gen_report import gen_report
from t4mon.logger import init_logger

from t4mon.collector import Collector
from t4mon.orchestrator import Orchestrator
from t4mon import arguments


__version__ = get_versions()['version']
__author__ = 'fernandezjm'
__all__ = ('main',
           'dump_config')
del get_versions


def dump_config(output=None, **kwargs):
    """
    Dump current configuration to screen, useful for creating a new
    ``settings.cfg`` file

    Arguments:
        output (Optional[str]): output filename, stdout if None
    """
    conf = arguments.read_config(**kwargs)
    conf.write(output or sys.stdout)


def main():  # pragma: no cover
    """
    Get input arguments and pass it to Orchestrator
    """
    sys_arguments = sys.argv[1:]
    arguments_ = arguments._parse_arguments_cli(sys_arguments)
    if arguments_.get('config', False):
        dump_config(**arguments_)
        return
    for par in ['local', 'localcsv']:
        if arguments_.get(par, False):
            sys_arguments.remove('--{0}'.format(par))
            create_reports_from_local(
                sys_arguments,
                prog='{0} --{1}'.format(sys.argv[0], par),
                pkl=par is 'local'
            )
            return
    arguments_ = arguments._parse_arguments_main(
        sys_arguments if sys_arguments else ['--dummy']
    )
    _orchestrator = Orchestrator(**arguments_)
    _orchestrator.start()

# def gui():  # pragma: no cover
    # """ Graphical interface for main """
    # sys_arguments = sys.argv[1:]
    # arguments_ = arguments._parse_arguments_gui(sys_arguments)
    # _orchestrator = Orchestrator(**arguments_)
    # _orchestrator.start()


def create_reports_from_local(cli_arguments,
                              prog=None,
                              pkl=True):  # pragma: no cover
    """
    Create HTML reports from locally stored data
    """
    arguments_ = arguments._parse_arguments_local(cli_arguments,
                                                  prog=prog,
                                                  pkl=pkl)
    _orchestrator = Orchestrator(**arguments_)
    argument_file_name = '{0}_file'.format('pkl' if pkl else 'csv')
    _orchestrator.create_reports_from_local(arguments_.pop(argument_file_name),
                                            pkl=pkl,
                                            **arguments_)


if __name__ == "__main__":
    main()
