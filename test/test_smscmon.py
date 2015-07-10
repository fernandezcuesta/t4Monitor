#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import, print_function

import unittest
import ConfigParser
import socket
import Queue
import logging
from os import path

import paramiko
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

from pysmscmon import smscmon as smsc
from pysmscmon import df_tools
from pysmscmon import logger
from pysmscmon.sshtunnels.sftpsession import SftpSession, SFTPSessionError

TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')
TEST_CSV = 'test/test_data.csv'
TEST_PKL = 'test/test_data.pkl'
TEST_CONFIG = 'test/test_settings.cfg'
MY_DIR = path.dirname(path.abspath(__file__))


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
        monitor = smsc.SMSCMonitor(settings_file=TEST_CONFIG, logger=LOGGER)
        monitor.conf.set('DEFAULT', 'folder', MY_DIR)
        monitor.init_tunnels()
        # before start, tunnel should not be started
        self.assertFalse(monitor.server.is_started)
        # Stopping it should not do any harm
        self.assertIsNone(monitor.stop_server())
        # Start and check tunnel ports
        self.assertIsNone(monitor.start_server())  # starting should be silent
        self.assertTrue(monitor.server.is_started)
        self.assertIsInstance(monitor.server.tunnel_is_up, dict)
        for port in monitor.server.tunnel_is_up:
            self.assertTrue(monitor.server.tunnel_is_up[port])

        monitor.stop_server()

    def test_getsyslogs(self):
        """ Test function for get_system_logs """
        pass

    def test_getstatsfromhost(self):
        """ Test function for get_stats_from_host """
        test_system_id = 'System_1'
        monitor = smsc.SMSCMonitor(settings_file=TEST_CONFIG, logger=LOGGER)
        monitor.init_tunnels()
        monitor.start_server()
        monitor.conf.set('DEFAULT', 'folder', MY_DIR)
        with SftpSession(
            hostname='127.0.0.1',
            ssh_port=monitor.server.tunnelports[test_system_id]
                         ) as s:
            data = monitor.get_stats_from_host(
                monitor.conf.get(test_system_id, 'ip_or_hostname'),
                ['.csv'],
                sftp_client=s.sftp_session,
                logger=LOGGER,
                files_folder=monitor.conf.get(test_system_id, 'folder')
                                                )

            should_be_empty_data = monitor.get_stats_from_host(
                monitor.conf.get(test_system_id, 'ip_or_hostname'),
                ['i_do_not_exist'],
                sftp_client=s.sftp_session,
                logger=LOGGER,
                files_folder=monitor.conf.get(test_system_id, 'folder')
                                                )

        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)
        self.assertTrue(should_be_empty_data.empty)
        monitor.stop_server()
#TODO: Test with the other connection method (not giving a session)
#TODO: Test with localfs access

    def test_getsysdata(self):
        """ Test function for get_system_data """
        test_system_id = 'System_1'
        with smsc.SMSCMonitor(settings_file=TEST_CONFIG,
                              logger=LOGGER) as monitor:
            monitor.alldays = True  # Ignore timestamp on test data
            monitor.conf.set('DEFAULT', 'folder', MY_DIR)
            with SftpSession(
                hostname='127.0.0.1',
                ssh_port=monitor.server.tunnelports[test_system_id]
                             ) as s:
                data = monitor.get_system_data(system=test_system_id,
                                               session=s.sftp_session)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)
        # monitor.stop_server()

    # def test_collectsysdata(self):
    #     """ Test function for collect_system_data """
    #     monitor = smsc.SMSCMonitor(settings_file=TEST_CONFIG, logger=LOGGER)
    #     monitor.alldays = True  # Ignore timestamp on test data
    #     monitor.init_tunnels()
    #     monitor.start_server()
    #     (data, logs) = monitor.collect_system_data(system='System_1')
    #     monitor.stop_server()
    #     pass

    def test_serialmain(self):
        """ Test function for main (serial mode) """
        pass

    def test_threadedmain(self):
        """ Test function for main (threaded mode) """
        pass
