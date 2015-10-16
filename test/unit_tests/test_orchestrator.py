#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import os

import tempfile
from datetime import datetime as dt

import pandas as pd
from pandas.util.testing import assert_frame_equal

from t4mon.orchestrator import Orchestrator
from t4mon import collector

from .base import (
    LOGGER,
    TEST_CSV,
    TEST_PKL,
    BAD_CONFIG,
    TEST_CONFIG,
    BaseTestClass
)


class TestOrchestrator(BaseTestClass):
    """ Set of test functions for orchestrator.py """

    def test_get_absolute_path(self):
        """ Test auxiliary function get_absolute_path """
        self.assertEqual(
            self.container.get_absolute_path(),
            os.path.dirname(os.path.abspath(TEST_CONFIG)) + os.sep
        )
        self.assertEqual(
            self.container.get_absolute_path('/home/user/file.txt'),
            '/home/user/file.txt'
        )

    def test_container(self):
        """ Check that Container has the correct fields by default """
        self.assertDictEqual(self.container.graphs, {})
        self.assertDictEqual(self.container.logs,
                             {'my_sys': 'These are my dummy log results'}
                             )
        self.assertEqual(dt.today().year, self.container.year)
        self.assertGreaterEqual(dt.today().toordinal(),
                                dt.strptime(self.container.date_time,
                                            '%d/%m/%Y %H:%M:%S').toordinal())
        assert_frame_equal(pd.DataFrame(), self.container.data)
        self.assertEqual(self.container.system, '')
        self.assertIn('html_template', self.container.__dict__)
        self.assertIn('graphs_file', self.container.__dict__)
        self.assertNotEqual(self.container.store_folder, '')
        self.assertNotEqual(self.container.reports_folder,
                            self.container.store_folder)
        self.assertIsInstance(self.container.__str__(), str)

    def test_clone(self):
        """ Test function for Orchestrator.clone() """
        container_clone = self.container.clone(system='my_sys')
        self.assertEqual(self.container.date_time,
                         container_clone.date_time)

    def test_check_files(self):
        """ Test check_files"""
        # First of all, check with default settings
        self.assertIsNone(self.container.check_files())

        self.container.settings_file = TEST_CONFIG
        self.assertIsNone(self.container.check_files())

        # Check with wrong settings file
        with self.assertRaises(collector.ConfigReadError):
            self.container.settings_file = BAD_CONFIG
            self.container.check_files()
        with self.assertRaises(collector.ConfigReadError):
            self.container.settings_file = TEST_CSV
            self.container.check_files()

        # Check with missing settings file
        with self.assertRaises(collector.ConfigReadError):
            self.container.settings_file = 'test/unexisting.file'
            self.container.check_files()

    def test_reports_generator(self):
        """ Test function for Orchestrator.reports_generator() """
        collector.add_methods_to_pandas_dataframe(logger=LOGGER)
        container = self.container.clone()
        container.data = pd.read_pickle(TEST_PKL)
        container.reports_generator()
        # Test the threaded version
        container.threaded = True
        container.data.system.add('SYS2')
        container.reports_generator()
