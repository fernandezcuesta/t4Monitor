#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for orchestrator.py
"""
from __future__ import absolute_import

import os
from datetime import datetime as dt
import unittest

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
            self.orchestrator_test.get_absolute_path(),
            os.path.dirname(os.path.abspath(TEST_CONFIG)) + os.sep
        )
        self.assertEqual(
            self.orchestrator_test.get_absolute_path('/home/user/file.txt'),
            '/home/user/file.txt'
        )

    def test_orchestrator(self):
        """ Check that Orchestrator has the correct fields by default """
        self.assertDictEqual(self.orchestrator_test.graphs, {})
        self.assertDictEqual(self.orchestrator_test.logs,
                             {'my_sys': 'These are my dummy log results'}
                             )
        self.assertGreaterEqual(dt.today().toordinal(),
                                dt.strptime(self.orchestrator_test.date_time,
                                            '%d/%m/%Y %H:%M:%S').toordinal())
        assert_frame_equal(pd.DataFrame(), self.orchestrator_test.data)
        self.assertIn('html_template', self.orchestrator_test.__dict__)
        self.assertIn('graphs_definition_file',
                      self.orchestrator_test.__dict__)
        self.assertNotEqual(self.orchestrator_test.store_folder, '')
        self.assertNotEqual(self.orchestrator_test.reports_folder,
                            self.orchestrator_test.store_folder)
        self.assertIsInstance(self.orchestrator_test.__str__(), str)

    def test_clone(self):
        """ Test function for Orchestrator.clone() """
        container_clone = self.orchestrator_test.clone(system='my_sys')
        self.assertEqual(self.orchestrator_test.date_time,
                         container_clone.date_time)

    def test_check_files(self):
        """ Test check_files"""
        test_container = self.orchestrator_test.clone()
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
        container = self.orchestrator_test.clone()
        container.data = pd.read_pickle(TEST_PKL)
        container.reports_generator()
        self.assertNotEqual(container.reports_written, [])
        for report_file in container.reports_written:
            self.assertTrue(os.path.exists(report_file))
        # Test the non-threaded version
        container.safe = True
        container.data.system.add('SYS2')
        container.reports_generator()
        self.assertNotEqual(container.reports_written, [])
        for report_file in container.reports_written:
            self.assertTrue(os.path.exists(report_file))

    def test_create_reports_from_local_pkl(self):
        """
        Test function for Orchestrator.create_reports_from_local_pkl()
        """
        _orchestrator = self.orchestrator_test.clone()
        _orchestrator.create_reports_from_local_pkl(TEST_PKL)
        self.assertNotEqual(_orchestrator.reports_written, [])
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))

    def test_create_reports_from_local_csv(self):
        """ Test function for Orchestrator.create_reports_from_local_csv() """
        _orchestrator = self.orchestrator_test.clone()
        _orchestrator.create_reports_from_local_csv(TEST_CSV)
        self.assertNotEqual(_orchestrator.reports_written, [])
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))
