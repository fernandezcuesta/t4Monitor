#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import, print_function

import unittest
import ConfigParser
import logging
import socket
from os import path

import paramiko
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

from pysmscmon import smscmon as smsc
from pysmscmon import df_tools
from pysmscmon import logger

TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')
TEST_CSV = 'test/test_data.csv'
TEST_PKL = 'test/test_data.pkl'
TEST_CONFIG = 'test/test_settings.cfg'

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

    def test_initlogger(self):
        """ Test function for init_logger """
        # Default options, loglevel is 20 (INFO)
        my_logger = logger.init_logger()
        self.assertEqual(my_logger.level, 20)
        self.assertEqual(my_logger.name, logger.__name__)
        my_logger = logger.init_logger(loglevel='DEBUG', name='testset')
        self.assertEqual('DEBUG', logging.getLevelName(my_logger.level))
        self.assertEqual('testset', my_logger.name)

    def test_getstats(self):
        """ Test function for get_stats_from_host """
        df1 = smsc.get_stats_from_host('localfs',
                                       TEST_CSV,
                                       logger=LOGGER)
        df2 = df_tools.read_pickle(TEST_PKL)

        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        assert_frame_equal(df1, df2)

    def test_sdata(self):
        """ Test methods related to SData class """
        sdata = smsc.SData()
        # first of all, check default values
        self.assertIsNone(sdata.server)
        self.assertEqual(sdata.system, '')
        self.assertIsNone(sdata.conf)
        self.assertFalse(sdata.alldays)
        self.assertFalse(sdata.nologs)
        self.assertIsNone(sdata.logger)
        self.assertIsNone(sdata.settings_file)

        # fill it with some data, clone and test contents of copy
        sdata.system = 'TEST'
        sdata.alldays = True
        sdata.logger = LOGGER
        sdata.settings_file = 'test/test_settings.cfg'

        clone = sdata.clone(system='SYSTEM2')
        self.assertIs(clone.system, 'SYSTEM2')
        self.assertEqual(sdata.server, clone.server)
        self.assertEqual(sdata.conf, clone.conf)
        self.assertEqual(sdata.alldays, clone.alldays)
        self.assertEqual(sdata.nologs, clone.nologs)
        self.assertEqual(sdata.logger, clone.logger)
        self.assertEqual(sdata.settings_file, clone.settings_file)

        self.assertIn('Settings file: {}'.format(sdata.settings_file),
                      sdata.__str__())


class TestSmscmon_Ssh(unittest.TestCase):
    """ Set of test functions for interactive (ssh) methods of smscmon.py """
    @classmethod
    def setUpClass(cls):
        smsc.add_methods_to_pandas_dataframe(LOGGER)
        # Check if SSH is listening in localhost """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect('localhost')
            ssh.open_sftp()
            ssh.close()
        except (paramiko.BadHostKeyException, paramiko.AuthenticationException,
                paramiko.SSHException, socket.error, socket.gaierror) as exc:
            print('Exception while trying to setup interactive SSH tests. '
                  'It is assumed that SSH to localhost is setup with pkey and '
                  'settings are configured in ~/.ssh/config for "localhost".\n'
                  'ERROR: %s', exc)
            raise unittest.SkipTest

    def test_inittunnels(self):
        """ Test function for init_tunnels """
        my_dir = path.dirname(path.abspath(__file__))
        my_config = smsc.read_config(TEST_CONFIG)
        my_config.set('DEFAULT', 'folder', my_dir)
        my_tunnels = smsc.init_tunnels(my_config, LOGGER)
        # before start, tunnel should not be started
        self.assertFalse(my_tunnels.is_started)
        # Stopping it should not do any harm
        self.assertIsNone(my_tunnels.stop())
        # Start and check tunnel ports
        self.assertIsNone(my_tunnels.start())
        self.assertTrue(my_tunnels.is_started)
        self.assertIsInstance(my_tunnels.tunnel_is_up, dict)
        for port in my_tunnels.tunnel_is_up:
            self.assertTrue(my_tunnels.tunnel_is_up[port])

        my_tunnels.stop()

    def test_collectsysdata(self):
        """ Test function for collect_system_data """
        pass

    def test_getsyslogs(self):
        """ Test function for get_system_logs """
        pass

    def test_getsysdata(self):
        """ Test function for get_system_data """
        pass

    def test_serialmain(self):
        """ Test function for main (serial mode) """
        pass

    def test_threadedmain(self):
        """ Test function for main (threaded mode) """
        pass
