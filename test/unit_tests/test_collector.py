#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - SMSC monitoring **test functions**
"""
from __future__ import print_function, absolute_import

import Queue
import logging
import unittest
import ConfigParser

import pandas as pd
from pandas.util.testing import assert_frame_equal

from t4mon import collector
from t4mon import df_tools
from t4mon.sshtunnels.sftpsession import SftpSession

from .base import LOGGER, TEST_CSV, TEST_PKL, TEST_CONFIG


class TestCollector(unittest.TestCase):
    """ Set of test functions for collector.py """
    @classmethod
    def setUpClass(cls):
        collector.add_methods_to_pandas_dataframe(LOGGER)

    def test_config(self):
        """ test function for read_config """
        config = collector.read_config(TEST_CONFIG)
        self.assertIsInstance(config, ConfigParser.SafeConfigParser)
        self.assertGreater(config.sections(), 2)
        self.assertIn('GATEWAY', config.sections())
        self.assertTrue(all([key in [i[0] for i in config.items('DEFAULT')]
                             for key in ['ssh_port', 'ssh_timeout',
                                         'tunnel_port', 'folder', 'username',
                                         'ip_or_hostname']]))
        # Trying to read a bad formatted config file should raise an exception
        self.assertRaises(collector.ConfigReadError,
                          collector.read_config,
                          TEST_CSV)

    def test_getstats(self):
        """ Test function for get_stats_from_host """
        monitor = collector.Collector()
        df1 = monitor.get_stats_from_host(filespec_list=TEST_CSV)
        df2 = df_tools.read_pickle(TEST_PKL)

        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        assert_frame_equal(df1, df2)

    def test_Collector_class(self):
        """ Test methods related to the Collector class """
        sdata = collector.Collector()
        # first of all, check default values
        self.assertIsNone(sdata.server)
        self.assertIsInstance(sdata.results_queue, Queue.Queue)
        self.assertIsInstance(sdata.conf, ConfigParser.SafeConfigParser)
        self.assertFalse(sdata.alldays)
        self.assertFalse(sdata.nologs)
        self.assertIsInstance(sdata.logger, logging.Logger)
        self.assertEqual(sdata.settings_file, collector.DEFAULT_SETTINGS_FILE)
        self.assertIsInstance(sdata.data, pd.DataFrame)
        self.assertDictEqual(sdata.logs, {})

        # fill it with some data, clone and test contents of copy
        sdata.alldays = True
        sdata.logger = LOGGER
        sdata.settings_file = TEST_CONFIG

        clone = sdata.clone()
        self.assertEqual(sdata.server, clone.server)
        self.assertEqual(sdata.conf, clone.conf)
        self.assertEqual(sdata.alldays, clone.alldays)
        self.assertEqual(sdata.nologs, clone.nologs)
        self.assertEqual(sdata.logger, clone.logger)
        self.assertEqual(sdata.settings_file, clone.settings_file)

        self.assertIn('Settings file: {}'.format(sdata.settings_file),
                      sdata.__str__())
