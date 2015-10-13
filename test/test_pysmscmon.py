#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import os
import tempfile
import unittest
from datetime import datetime as dt

import pandas as pd
from pandas.util.testing import assert_frame_equal

import pysmscmon as init_func
from pysmscmon import smscmon as smsc

from .interactive_common import *

BAD_CONFIG = 'test/test_settings_BAD.cfg'


class TestInit(unittest.TestCase):
    """ Set of test functions for __init__.py """

    def test_get_absolute_path(self):
        """ Test auxiliary function get_absolute_path """
        self.assertEqual(init_func.get_absolute_path(),
                         os.path.dirname(smsc.DEFAULT_SETTINGS_FILE) + os.sep)
        self.assertEqual(init_func.get_absolute_path(__file__),
                         __file__)

    def test_container(self):
        """ Check that Container has the correct fields by default """
        container = init_func.Container()
        self.assertDictEqual(container.graphs, {})
        self.assertDictEqual(container.graphs, container.logs)
        self.assertEqual(dt.today().year, container.year)
        self.assertGreaterEqual(dt.today().toordinal(),
                                dt.strptime(container.date_time,
                                            '%d/%m/%Y %H:%M:%S').toordinal())
        assert_frame_equal(pd.DataFrame(), container.data)
        self.assertEqual(container.system, '')
        self.assertEqual(container.html_template, '')
        self.assertEqual(container.graphs_file, '')
        self.assertNotEqual(container.store_folder, '')
        self.assertNotEqual(container.reports_folder, container.store_folder)
        self.assertIsInstance(container.__str__(), str)

        # Now check that no logger is created when loglevel is 'keep', so
        # trying to get the representation of the object raises an error,
        # since container.logger.level doesn't exist yet.
        container = init_func.Container(loglevel='keep')
        self.assertIsNone(container.logger)
        self.assertRaises(AttributeError, container.__str__)

    def test_check_config(self):
        """ Test function for check_config """
        pass

    def test_check_files(self):
        """ Test check_files"""
        # First of all, check with default settings
        container = init_func.Container()
        self.assertTrue(container.check_files())
        self.assertTrue(container.check_files(TEST_CONFIG))
        # Check with wrong settings file
        self.assertFalse(container.check_files(BAD_CONFIG))
        self.assertFalse(container.check_files(TEST_CSV))
        # Check with missing settings file
        self.assertFalse(container.check_files('test/unexisting.file'))

    def test_dump_config(self):
        """ Test function for dump_config """
        with tempfile.SpooledTemporaryFile() as temp_file:
            init_func.dump_config(temp_file)
            temp_file.seek(0)
            config_dump = temp_file.readlines()

        self.assertGreater(len(config_dump), 0)
        self.assertTrue(any(['DEFAULT' in line for line in config_dump]))

    def test_clone(self):
        """ Test function for Container.clone() """
        container = init_func.Container()
        container.logs['my_sys'] = 'These are my dummy log results'
        container_clone = container.clone(system='my_sys')
        self.assertEqual(container.date_time, container_clone.date_time)

    def test_generate_reports(self):
        """ Test function for Container.test_generate_reports() """
        container = init_func.Container()
        smsc.add_methods_to_pandas_dataframe()
        container.data = pd.read_pickle(TEST_PKL)
        container.generate_reports(container.data.system)
        # Test the threaded version
        container.threaded = True
        container.data.system = [container.data.system, 'SYS2']
        container.generate_reports(container.data.system)

    def test_argument_parse(self):
        parser = init_func.argument_parse(
            ['--all',
             '--settings={}'.format(BAD_CONFIG),
             '--loglevel=DEBUG']
        )
        self.assertTrue(parser['alldays'])
        self.assertEqual(parser['loglevel'], 'DEBUG')
        self.assertFalse(parser['nologs'])
        self.assertFalse(parser['noreports'])
        self.assertEqual(parser['settings_file'], BAD_CONFIG)
        self.assertFalse(parser['threaded'])


class TestMain(TestWithSsh):
    """ Set of test functions for interactive (ssh) methods of __init__.py """

    def test_start(self):
        """ Test function for main() and start() """

        init_func.start(alldays=True, settings_file=TEST_CONFIG, threads=True)
        # Returns nothing if non-existing or bad settings file
        self.assertIsNone(init_func.start(settings_file=BAD_CONFIG))
