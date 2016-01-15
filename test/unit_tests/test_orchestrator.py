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

from t4mon import df_tools, collector
from t4mon.orchestrator import Orchestrator

from .base import TEST_CSV, TEST_PKL, BAD_CONFIG, TEST_CONFIG, BaseTestClass


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
        self.assertDictEqual(self.orchestrator_test.logs,
                             {'my_sys': 'These are my dummy log results'}
                             )
        self.assertGreaterEqual(dt.today().toordinal(),
                                dt.strptime(self.orchestrator_test.date_time,
                                            '%d/%m/%Y %H:%M:%S').toordinal())
        assert_frame_equal(pd.DataFrame(),
                           self.orchestrator_test.data)
        self.assertIn('html_template', self.orchestrator_test.__dict__)
        self.assertIn('graphs_definition_file',
                      self.orchestrator_test.__dict__)
        self.assertNotEqual(self.orchestrator_test.store_folder, '')
        self.assertNotEqual(self.orchestrator_test.reports_folder,
                            self.orchestrator_test.store_folder)
        self.assertIsInstance(self.orchestrator_test.__str__(), str)

    def test_clone(self):
        """ Test function for Orchestrator.clone() """
        _orchestrator = self.orchestrator_test.clone()
        self.assertEqual(self.orchestrator_test.date_time,
                         _orchestrator.date_time)

    def test_check_files(self):
        """ Test check_files"""
        _orchestrator = self.orchestrator_test.clone()
        # First of all, check with default settings
        self.assertIsNone(_orchestrator.check_files())

        # Check with wrong settings file
        with self.assertRaises(collector.ConfigReadError):
            _orchestrator.settings_file = BAD_CONFIG
            _orchestrator.check_files()
        with self.assertRaises(collector.ConfigReadError):
            _orchestrator.settings_file = TEST_CSV
            _orchestrator.check_files()

        # Check with missing settings file
        with self.assertRaises(collector.ConfigReadError):
            _orchestrator.settings_file = 'test/non_existing.file'
            _orchestrator.check_files()

    def test_check_files_raises_exception_if_bad_settings(self):
        """ Check that if the setting file contains a link to a
        non existing file, init will raise an exception """
        with self.assertRaises(collector.ConfigReadError):
            Orchestrator(settings_file=BAD_CONFIG).check_files()

    def test_reports_generator(self):
        """ Test function for Orchestrator.reports_generator() """
        _orchestrator = self.orchestrator_test.clone()
        _orchestrator.data = self.test_data
        _orchestrator.reports_generator()
        self.assertNotEqual(_orchestrator.reports_written, [])
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))
        # Test the non-threaded version
        _orchestrator.reports_written = []  # reset the count
        _orchestrator.safe = True
        _orchestrator.data = df_tools.consolidate_data(
            partial_dataframe=self.test_data,
            dataframe=self.test_data,
            system='SYS2'
        )
        _orchestrator.reports_generator()
        self.assertNotEqual(_orchestrator.reports_written, [])
        self.assertEqual(len(_orchestrator.reports_written), 2)
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))

    def test_create_reports_from_local(self):
        """
        Test function for Orchestrator.create_reports_from_local(pkl=True)
        """
        _orchestrator = self.orchestrator_test.clone()
        _orchestrator.create_reports_from_local(TEST_PKL)
        self.assertNotEqual(_orchestrator.reports_written, [])
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))
        # Non existing file raises error
        with self.assertRaises(IOError):
            _orchestrator.create_reports_from_local('WR0NG')

    def test_create_reports_from_local_csv(self):
        """
        Test function for Orchestrator.create_reports_from_local(pkl=False)
        """
        _orchestrator = self.orchestrator_test.clone()
        _orchestrator.create_reports_from_local(TEST_CSV, pkl=False)
        self.assertNotEqual(_orchestrator.reports_written, [])
        for report_file in _orchestrator.reports_written:
            self.assertTrue(os.path.exists(report_file))
        # Non existing file raises error
        with self.assertRaises(IOError):
            _orchestrator.create_reports_from_local('WR0NG', pkl=False)

    def test_local_store(self):
        """ Test that data can be stored locally in both PKL and CSV formats
        """
        _orchestrator = self.orchestrator_test.clone()
        _collector = self.collector_test.clone()
        _collector.data = self.test_data
        _orchestrator.local_store(_collector)
        for extension in ['pkl.gz', 'csv']:
            filename = '{0}/data_{1}.{2}'.format(_orchestrator.store_folder,
                                                 _orchestrator.date_tag(),
                                                 extension)
            self.assertTrue(os.path.exists(filename))
