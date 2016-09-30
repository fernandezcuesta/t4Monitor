#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring functional tests
"""

from __future__ import print_function, absolute_import

import re
import tempfile
from os import sep
from test.functional_tests import base as b

import pandas as pd
import pytest
from t4mon import collector
from sshtunnels.sftpsession import SftpSession


class TestOrchestrator(b.TestWithTempConfig):

    """
    Set of test functions for interactive (ssh) methods of orchestrator.py
    """

    @pytest.mark.timeout(timeout=60)  # disable timeout, slowest test
    def test_orchestrator_start(self):
        """ Test function for Orchestrator.start() """
        # Orchestrator needs a file with the settings, and the files linked in
        # that settings file may be relative to the settings file location, so
        # work in a temporary directory
        for safe_mode in [True, False]:
            orch = self.sandbox
            orch.safe = safe_mode
            with tempfile.NamedTemporaryFile(mode='w') as temp_config:
                orch.conf.write(temp_config)
                temp_config.seek(0)
                orch.settings_file = temp_config.name
                orch.start()
            for system in orch.systems:
                if system:
                    self.assertIn(
                        '{0}/Report_{1}_{2}.html'.format(orch.reports_folder,
                                                         orch.date_tag(),
                                                         system),
                        orch.reports_written
                    )


class TestCollector(b.TestWithSsh):

    """ Set of test functions for interactive (ssh) methods of collector.py """

    def test_init_tunnels(self):
        """ Test function for init_tunnels """
        monitor = self.sandbox.collector
        monitor.init_tunnels()
        # after init, tunnel should be already started
        self.assertTrue(monitor.server.is_alive)
        # Starting again should be silent
        self.assertIsNone(monitor._start_server())
        self.assertTrue(monitor.server.is_alive)
        self.assertIsInstance(monitor.server.tunnel_is_up, dict)
        for port in monitor.server.tunnel_is_up:
            self.assertTrue(monitor.server.tunnel_is_up[port])
        monitor.stop_server()

        # Should log an error if local ports aren't unique
        monitor = self.sandbox.collector.clone()
        monitor.conf.set('DEFAULT', 'tunnel_port', '22000')
        with monitor:
            self.assertIn("Couldn't open tunnel :22000 <> 127.0.0.1:22 "
                          "might be in use or destination not reachable",
                          self.test_log_messages['error'][-1])

    def test_collector_start(self):
        """ Test function for Collector.start """
        monitor = collector.Collector(settings_file=b.TEST_CONFIG,
                                      logger=self.logger,
                                      alldays=True,
                                      nologs=True,
                                      safe=False)
        monitor.start()
        # main reads by itself the configuration file, where the folder is not
        # set, thus won't find the files and return an empty dataframe
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertTrue(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)

        # Same by calling the threaded version
        monitor = collector.Collector(alldays=True,
                                      logger=self.logger,
                                      nologs=True,
                                      settings_file=b.TEST_CONFIG,
                                      safe=True)
        monitor.start()
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertTrue(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)

    def test_get_stats_from_host(self):
        """ Test function for get_stats_from_host """
        test_system_id = 'System_1'
        monitor = self.sandbox.collector
        monitor.init_tunnels()
        monitor.conf.set('DEFAULT', 'folder', b.MY_DIR)
        with SftpSession(
            hostname='127.0.0.1',
            ssh_port=monitor.server.tunnelports[test_system_id]
        ) as s:
            data = monitor.get_stats_from_host(
                filespec_list=['.csv'],
                hostname=test_system_id,
                sftp_session=s,
                logger=self.logger,
                files_folder=monitor.conf.get(test_system_id, 'folder')
            )
            should_be_empty_data = monitor.get_stats_from_host(
                filespec_list=['i_do_not_exist/'],
                hostname=test_system_id,
                sftp_session=s,
                logger=self.logger,
                files_folder=monitor.conf.get(test_system_id, 'folder') + sep
            )
        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)
        self.assertTrue(should_be_empty_data.empty)
        monitor.stop_server()

    def test_get_system_data(self):
        """ Test function for get_system_data """
        system_id = 'System_1'
        with self.sandbox.collector as monitor:
            for alldays in [True, False]:
                monitor.alldays = alldays  # Ignore timestamp on test data
                monitor.conf.set('DEFAULT', 'folder', b.MY_DIR)
                with SftpSession(
                    hostname='127.0.0.1',
                    ssh_port=monitor.server.tunnelports[system_id],
                    logger=self.logger
                ) as s:
                    data = monitor.get_system_data(system=system_id,
                                                   session=s)
                    # When the folder does not exist it should return empty df
                    monitor.conf.set('DEFAULT', 'folder', 'do-not-exist')
                    self.assertTrue(monitor.get_system_data(system=system_id,
                                                            session=s).empty)
                self.assertIsInstance(data, pd.DataFrame)
                self.assertNotEqual(data.empty, alldays)

    def test_get_system_logs(self):
        """ Test function for get_system_logs """
        test_system_id = 'System_1'
        with self.sandbox.collector as col:
            with SftpSession(
                hostname='127.0.0.1',
                logger=self.logger,
                ssh_port=col.server.tunnelports[test_system_id]
            ) as s:
                logs = col.get_system_logs(
                    ssh_session=s.ssh_transport,
                    system=test_system_id,
                    command='netstat -nrt'
                )
                self.assertIn('0.0.0.0', ''.join(logs))
                logs = col.get_system_logs(
                    ssh_session=s.get_channel(),
                    system=test_system_id
                )
                self.assertIsNone(logs)

    def test_get_data_and_logs(self):
        """ Test function for get_data_and_logs """
        with self.sandbox.collector as monitor:
            monitor.alldays = True  # Ignore timestamp on test data
            monitor.nologs = False

            monitor.conf.set('MISC', 'remote_log_cmd', 'netstat -nrt')
            monitor.get_data_and_logs(system='System_1')
            self.assertFalse(monitor.data.empty)
            self.assertNotIn('Log collection omitted',
                             monitor.logs['System_1'])
            monitor.nologs = True  # Skip log collection
            monitor.conf.set('DEFAULT', 'folder', '')

            monitor.get_data_and_logs(system='System_2')
            # sftp folder was not set so it shouldn't be any data for System_2
            self.assertTrue(monitor.select(system='System_2').empty)
            self.assertIn('Log collection omitted',
                          monitor.logs['System_2'])

            # Now test with an non-existing system
            monitor.get_data_and_logs(system='wrong_system_id')
            self.assertTrue(monitor.select(system='wrong_system_id').empty)

    def test_serial_handler(self):
        """ Test function for _serial_handler (AKA safe mode) """
        self.sandbox.collector._serial_handler()
        self.assertIsInstance(self.sandbox.collector.data, pd.DataFrame)
        self.assertFalse(self.sandbox.collector.data.empty)
        self.assertIsInstance(self.sandbox.collector.logs, dict)

    def test_threaded_handler(self):
        """ Test function for _threaded_handler (AKA fast mode) """
        monitor = collector.Collector(settings_file=b.TEST_CONFIG,
                                      logger=self.logger,
                                      alldays=True,
                                      nologs=True)
        monitor.conf.set('DEFAULT', 'folder', b.MY_DIR)
        monitor._threaded_handler()
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertFalse(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)
