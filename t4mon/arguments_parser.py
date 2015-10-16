#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import

import os
import sys
import argparse

from . import collector
from .logger import DEFAULT_LOGLEVEL

__all__ = ('parse_arguments',
           )


def get_input(text):
    return raw_input(text)


def parse_arguments(args=None):
    """ Argument parser for main method
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.
                                     RawTextHelpFormatter,
                                     description='SMSC Monitoring script (run'
                                                 ' smscmon-config to dump the'
                                                 ' configuration defaults)')
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
    # Default for collector is 'settings.cfg' in /conf
    if not args:
        parser.print_help()
        print('')
        while True:
            ans = get_input('No arguments were specified, continue with '
                            'defaults (check with smscmon-config)? ([Y]|n) ')
            if not ans or ans in ('y', 'Y'):
                print('Proceeding with default settings')
                break
            elif ans in ('n', 'N'):
                sys.exit('Aborting')
            print('Please enter y or n.')
    return vars(parser.parse_args(args))
