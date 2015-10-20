#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for orchestrator.py
"""
from __future__ import absolute_import

import os
from datetime import datetime as dt

import pandas as pd
from pandas.util.testing import assert_frame_equal

from t4mon import collector
from t4mon.orchestrator import Orchestrator

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
            self.orchestrator.get_absolute_path(),
            os.path.dirname(os.path.abspath(TEST_CONFIG)) + os.sep
        )
        self.assertEqual(
            self.orchestrator.get_absolute_path('/home/user/file.txt'),
            '/home/user/file.txt'
        )

    def test_orchestrator(self):
        """ Check that Orchestrator has the correct fields by default """
        self.assertDictEqual(self.orchestrator.graphs, {})
        self.assertDictEqual(self.orchestrator.logs,
                             {'my_sys': 'These are my dummy log results'}
                             )
        self.assertEqual(dt.today().year, self.orchestrator.year)
        self.assertGreaterEqual(dt.today().toordinal(),
                                dt.strptime(self.orchestrator.date_time,
                                            '%d/%m/%Y %H:%M:%S').toordinal())
        assert_frame_equal(pd.DataFrame(), self.orchestrator.data)
        self.assertEqual(self.orchestrator.system, '')
        self.assertIn('html_template', self.orchestrator.__dict__)
        self.assertIn('graphs_definition_file', self.orchestrator.__dict__)
        self.assertNotEqual(self.orchestrator.store_folder, '')
        self.assertNotEqual(self.orchestrator.reports_folder,
                            self.orchestrator.store_folder)
        self.assertIsInstance(self.orchestrator.__str__(), str)

    def test_clone(self):
        """ Test function for Orchestrator.clone() """
        container_clone = self.orchestrator.clone(system='my_sys')
        self.assertEqual(self.orchestrator.date_time,
                         container_clone.date_time)

    def test_check_files(self):
        """ Test check_files"""
        test_container = self.orchestrator.clone()
        # First of all, check with default settings
        self.assertIsNone(test_container.check_files())

        # Check with wrong settings file
        with self.assertRaises(collector.ConfigReadError):
            test_container.settings_file = BAD_CONFIG
            test_container.check_files()
        with self.assertRaises(collector.ConfigReadError):
            test_container.settings_file = TEST_CSV
            test_container.check_files()

        # Check with missing settings file
        with self.assertRaises(collector.ConfigReadError):
            test_container.settings_file = 'test/unexisting.file'
            test_container.check_files()

    def test_orchestrator_raises_exception_if_bad_settings(self):
        """ Check that if the setting file contains a link to a
        non existing file, init will raise an exception """
        with self.assertRaises(collector.ConfigReadError):
            Orchestrator(settings_file=BAD_CONFIG)

    def test_reports_generator(self):
        """ Test function for Orchestrator.reports_generator() """
        collector.add_methods_to_pandas_dataframe(logger=LOGGER)
        container = self.orchestrator.clone()
        container.data = pd.read_pickle(TEST_PKL)
        container.reports_generator()
        # Test the non-threaded version
        container.safe = True
        container.data.system.add('SYS2')
        container.reports_generator()
