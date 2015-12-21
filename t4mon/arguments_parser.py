#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import

import argparse
import logging
import sys

from . import collector
from .logger import DEFAULT_LOGLEVEL

__all__ = ('parse_arguments_local',
           'parse_arguments_main'
           )


DESCRIPTION = """
T4 collector and report generator script

Additional tools:
 --config: dump the configuration defaults
 --local: create reports from local data (typically under 'store/' folder)
 --localcsv: create reports from local CSV (typically under 'store/' folder)
"""


def get_input(text):
    return raw_input(text)


def check_for_sysargs(parser, args=None):
    """
    Check if relevant parameters were specified or ask the user to proceed
    with defaults
    """
    # Default for collector is 'settings.cfg' in /conf
    if not args:
        # parser.parse_args(args)
        parser.print_help()
        print('')
        while True:
            ans = get_input('No arguments were specified, continue with '
                            'defaults (check running with --config)? (y|[N]) ')
            if ans in ('y', 'Y'):
                print('Proceeding with default settings')
                break
            elif not ans or ans in ('n', 'N'):
                sys.exit('Aborting')
            print('Please enter y or n.')
    return vars(parser.parse_args(args))


def create_parser(args=None, prog=None):
    """
    Common parser parts for parse_arguments_local and parse_arguments_main
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.
                                     RawTextHelpFormatter,
                                     description=DESCRIPTION,
                                     prog=prog)
    parser.add_argument('--fast', action='store_false', dest='safe',
                        help='Threading mode, decrease execution time by 1/2, '
                             'but unstable under Windows environment')
    parser.add_argument(
        '--settings', dest='settings_file',
        default=collector.DEFAULT_SETTINGS_FILE,
        help='Settings file (check defaults with t4mon-config)'
        )
    parser.add_argument('--loglevel', default=DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        logging.getLevelName(DEFAULT_LOGLEVEL),
                        nargs='?')
    return parser


def parse_arguments_local(args=None, prog=None, pkl=True):
    """
    Argument parser for create_reports_from_local
    """
    parser = create_parser(prog=prog)
    filetype = 'pkl' if pkl else 'csv'
    parser.add_argument('{}_file'.format(filetype),
                        type=str,
                        metavar='input_{}_file'.format(filetype),
                        help='Pickle (optionally gzipped) data file' if pkl
                        else 'Plain CSV file')
    return check_for_sysargs(parser, args)


def parse_arguments_main(args=None):
    """
    Argument parser for main method
    """
    parser = create_parser()
    parser.add_argument('--all', action='store_true', dest='alldays',
                        help='Collect all data available on remote hosts'
                             'not just for today')
    parser.add_argument('--noreports', action='store_true',
                        help='Skip report creation, files are just gathered '
                             'and stored locally')
    parser.add_argument('--nologs', action='store_true',
                        help='Skip log collection from remote hosts')
    parser.add_argument('--config', action='store_true',
                        help='Show current configuration')
    parser.add_argument('--local', action='store_true',
                        help='Render a report from local data')
    parser.add_argument('--localcsv', action='store_true',
                        help='Make a report from local CSV data')
    parser.add_argument('dummy', type=str, nargs='?',
                        help=argparse.SUPPRESS)
    return check_for_sysargs(parser, args)
