#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for collector.py
"""
from __future__ import print_function, absolute_import

import Queue
import logging
import ConfigParser

import pandas as pd
import sshtunnel
from pandas.util.testing import assert_frame_equal

from t4mon import df_tools, collector

from .base import TEST_CSV, TEST_PKL, TEST_CONFIG, BaseTestClass


class TestCollector(BaseTestClass):

    """ Set of test functions for collector.py """

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
        df1 = self.collector_test.get_stats_from_host(filespec_list=TEST_CSV)
        df2 = df_tools.read_pickle(TEST_PKL)
        df1 = collector.consolidate_data(df1, df1, system=df2.system)

        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        assert_frame_equal(df1, df2)

    def test_Collector_class(self):
        """ Test methods related to the Collector class """
        # first of all, check default values
        my_collector = collector.Collector()
        self.assertIsNone(my_collector.server)
        self.assertIsInstance(my_collector.results_queue, Queue.Queue)
        self.assertIsInstance(my_collector.conf,
                              ConfigParser.SafeConfigParser)
        self.assertFalse(my_collector.alldays)
        self.assertFalse(my_collector.nologs)
        self.assertFalse(my_collector.safe)
        self.assertIsInstance(my_collector.logger,
                              logging.Logger)
        self.assertEqual(my_collector.settings_file,
                         collector.DEFAULT_SETTINGS_FILE)
        self.assertIsInstance(my_collector.data,
                              pd.DataFrame)
        self.assertDictEqual(my_collector.logs,
                             {})

        # Test also CollectorSandbox class methods
        coll_clone = self.collector_test.clone()
        coll_clone.alldays = coll_clone.safe = True
        self.assertNotEqual(self.collector_test.alldays,
                            coll_clone.alldays)
        self.assertNotEqual(self.collector_test.safe,
                            coll_clone.safe)

        self.assertEqual(self.collector_test.logger,
                         coll_clone.logger)
        self.assertEqual(self.collector_test.settings_file,
                         coll_clone.settings_file)
        self.assertEqual(self.collector_test.server,
                         coll_clone.server)
        self.assertEqual(self.collector_test.conf,
                         coll_clone.conf)
        self.assertEqual(self.collector_test.nologs,
                         coll_clone.nologs)

        self.assertIn('Settings file: {}'.format(
                      self.collector_test.settings_file
                      ),
                      self.collector_test.__str__())

    def tet_init_tunnels(self):
        """ Test Collector.init_tunnels() """
        with self.assertRaises(sshtunnel.BaseSSHTunnelForwarderError):
            with collector.Collector() as _col:
                for system in _col.systems:
                    print(system)
