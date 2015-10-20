#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 T4Monitor: T4-compliant CSV processor and visualizer for OpenVMS
 -----------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 t4 input_file
 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information

  line1: Collection date   (optional line)

  line2: Start time        (optional line)

  line3: Parameter Heading (comma separated)

 or

  ** Format 2: **

 line0: Header information containing T4 revision info and system information
 line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple `$`
 line2: parameter headings (comma separated)
 ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple `$`

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing
  hash and contents followed by it (sometimes even more samples...) is ignored

"""

from __future__ import print_function, absolute_import

import sys

from .collector import read_config
from .orchestrator import Orchestrator
from . arguments_parser import (parse_arguments_local_csv,
                                parse_arguments_local_pkl,
                                parse_arguments_main)


__version_info__ = (0, 9, 0)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'

__all__ = ('main',
           'dump_config')


def dump_config(output=None):
    """ Dump current configuration to screen, useful for creating a new
    settings.cfg file """
    conf = read_config()
    conf.write(output or sys.stdout)


def main():  # pragma: no cover
    _orchestrator = Orchestrator(**parse_arguments_main(sys.argv[1:]))
    _orchestrator.start()


def create_reports_from_local_pkl():  # pragma: no cover
    """ Create HTML reports from local stored PKL file """
    arguments = parse_arguments_local_pkl(sys.argv[1:])
    pkl_file = arguments.pop('pkl_file')
    _orchestrator = Orchestrator(**arguments)
    _orchestrator.create_reports_from_local_pkl(pkl_file)


def create_reports_from_local_csv():  # pragma: no cover
    """ Create HTML reports from local stored CSV files """
    arguments = parse_arguments_local_csv(sys.argv[1:])
    csv_file = arguments.pop('csv_file')
    _orchestrator = Orchestrator(**arguments)
    _orchestrator.create_reports_from_local_csv(csv_file)


if __name__ == "__main__":  # pragma: no cover
    main()
