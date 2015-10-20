#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import

import os
import sys
import argparse

from . import collector
from .logger import DEFAULT_LOGLEVEL

__all__ = ('parse_arguments_local_csv',
           'parse_arguments_local_pkl',
           'parse_arguments_main'
           )


DESCRIPTION = 'T4 collector and report generator script (run t4mon-config ' \
              'to dump the configuration defaults or t4mon-local to create ' \
              'reports from local data)'


def get_input(text):
    return raw_input(text)


def check_for_sysargs(parser, args=None):
    """ Check if relevant parameters were specified or ask the user to
    proceed with defaults
    """
    # Default for collector is 'settings.cfg' in /conf
    if not args:
        parser.parse_args(args)
        parser.print_help()
        print('')
        while True:
            ans = get_input('No arguments were specified, continue with '
                            'defaults (check with t4mon-config)? ([Y]|n) ')
            if not ans or ans in ('y', 'Y'):
                print('Proceeding with default settings')
                break
            elif ans in ('n', 'N'):
                sys.exit('Aborting')
            print('Please enter y or n.')
    return vars(parser.parse_args(args))


def create_parser(args=None):
    """ Common parser parts for parse_arguments_local and parse_arguments_main
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.
                                     RawTextHelpFormatter,
                                     description=DESCRIPTION)
    parser.add_argument('--safe', action='store_true', dest='safe',
                        help='Serial mode running without threads, '
                             'increasing execution time by 2x.')
    parser.add_argument(
        '--settings', dest='settings_file',
        default=collector.DEFAULT_SETTINGS_FILE,
        help='Settings file (default {})'.format(
            os.path.relpath(collector.DEFAULT_SETTINGS_FILE)
            )
        )
    parser.add_argument('--loglevel', default=DEFAULT_LOGLEVEL,
                        choices=['DEBUG',
                                 'INFO',
                                 'WARNING',
                                 'ERROR',
                                 'CRITICAL'],
                        help='Debug level (default: %s)' %
                        DEFAULT_LOGLEVEL,
                        nargs='?')
    return parser


def parse_arguments_local_pkl(args=None):
    """ Argument parser for create_reports_from_local_pkl
    """
    parser = create_parser()
    parser.add_argument('pkl_file',
                        type=str, metavar='input_pkl_file',
                        help='Pickle (optionally gzipped) data file')
    return check_for_sysargs(parser, args)


def parse_arguments_local_csv(args=None):
    """ Argument parser for create_reports_from_local_csv
    """
    parser = create_parser()
    parser.add_argument('csv_file',
                        type=str, metavar='input_csv_file',
                        help='Plain CSV file')
    return check_for_sysargs(parser, args)


def parse_arguments_main(args=None):
    """ Argument parser for main method
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
    return check_for_sysargs(parser, args)
