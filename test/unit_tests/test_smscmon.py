#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import print_function, absolute_import

import Queue
import socket
import logging
import unittest
import ConfigParser

import numpy as np
import pandas as pd
import paramiko
from pandas.util.testing import assert_frame_equal

from pysmscmon import smscmon as smsc
from pysmscmon import logger, df_tools
from pysmscmon.sshtunnels.sftpsession import SftpSession

from .base import LOGGER, TEST_CSV, TEST_PKL, TEST_CONFIG


class TestSmscmon(unittest.TestCase):
    """ Set of test functions for smscmon.py """
    @classmethod
    def setUpClass(cls):
        smsc.add_methods_to_pandas_dataframe(LOGGER)

    def test_config(self):
        """ test function for read_config """
        config = smsc.read_config(TEST_CONFIG)
        self.assertIsInstance(config, ConfigParser.SafeConfigParser)
        self.assertGreater(config.sections(), 2)
        self.assertIn('GATEWAY', config.sections())
        self.assertTrue(all([key in [i[0] for i in config.items('DEFAULT')]
                             for key in ['ssh_port', 'ssh_timeout',
                                         'tunnel_port', 'folder', 'username',
                                         'ip_or_hostname']]))
        # Trying to read a bad formatted config file should raise an exception
        self.assertRaises(smsc.ConfigReadError, smsc.read_config, TEST_CSV)

    def test_getstats(self):
        """ Test function for get_stats_from_host """
        monitor = smsc.SMSCMonitor()
        df1 = monitor.get_stats_from_host('localfs', TEST_CSV)
        df2 = df_tools.read_pickle(TEST_PKL)

        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        assert_frame_equal(df1, df2)

    def test_SMSCMonitor_class(self):
        """ Test methods related to SMSCMonitor class """
        sdata = smsc.SMSCMonitor()
        # first of all, check default values
        self.assertIsNone(sdata.server)
        self.assertIsInstance(sdata.results_queue, Queue.Queue)
        self.assertIsInstance(sdata.conf, ConfigParser.SafeConfigParser)
        self.assertFalse(sdata.alldays)
        self.assertFalse(sdata.nologs)
        self.assertIsInstance(sdata.logger, logging.Logger)
        self.assertEqual(sdata.settings_file, smsc.DEFAULT_SETTINGS_FILE)
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
