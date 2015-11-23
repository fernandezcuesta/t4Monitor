#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring functional tests
"""

from __future__ import print_function, absolute_import

import tempfile

import pandas as pd

from t4mon import collector
from sshtunnels.sftpsession import SftpSession

from test.functional_tests.base import *  # from .base import *


class TestOrchestrator(TestWithTempConfig):

    """
    Set of test functions for interactive (ssh) methods of orchestrator.py
    """

    def test_orchestrator_start(self):
        """ Test function for Orchestrator.start() """
        # Orchestrator needs a file with the settings, and the files linked in
        # that settings file may be relative to the settings file location, so
        # work in a temporary directory
        with tempfile.NamedTemporaryFile() as temp_config:
            self.sandbox.collector.conf.write(temp_config)
            temp_config.seek(0)
            orch = OrchestratorSandbox(loglevel='DEBUG',
                                       settings_file=temp_config.name,
                                       alldays=True,
                                       safe=True)
            orch.start()
        for system in orch.collector.systems:
            if system:
                self.assertIn(
                    '{0}/Report_{1}_{2}.html'.format(orch.reports_folder,
                                                     orch.date_tag(),
                                                     system),
                    orch.reports_written
                )


class TestCollector(TestWithSsh):

    """ Set of test functions for interactive (ssh) methods of collector.py """

    def test_inittunnels(self):
        """ Test function for init_tunnels """
        monitor = self.sandbox.collector
        monitor.init_tunnels()
        # after init, tunnel should be already started
        self.assertTrue(monitor.server._is_started)
        # Starting again should be silent
        self.assertIsNone(monitor.start_server())
        self.assertTrue(monitor.server._is_started)
        self.assertIsInstance(monitor.server.tunnel_is_up, dict)
        for port in monitor.server.tunnel_is_up:
            self.assertTrue(monitor.server.tunnel_is_up[port])
        monitor.stop_server()

    def test_getstatsfromhost(self):
        """ Test function for get_stats_from_host """
        test_system_id = 'System_1'
        monitor = self.sandbox.collector
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
                sftp_session=s,
                logger=LOGGER,
                files_folder=monitor.conf.get(test_system_id, 'folder')
                                                )

            should_be_empty_data = monitor.get_stats_from_host(
                monitor.conf.get(test_system_id, 'ip_or_hostname'),
                ['i_do_not_exist'],
                sftp_session=s,
                logger=LOGGER,
                files_folder=monitor.conf.get(test_system_id, 'folder')
                                                )

        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)
        self.assertTrue(should_be_empty_data.empty)
        monitor.stop_server()

    def test_getsysdata(self):
        """ Test function for get_system_data """
        test_system_id = 'System_1'
        with self.sandbox.collector as monitor:
            monitor.alldays = True  # Ignore timestamp on test data
            monitor.conf.set('DEFAULT', 'folder', MY_DIR)
            with SftpSession(
                hostname='127.0.0.1',
                ssh_port=monitor.server.tunnelports[test_system_id],
                logger=LOGGER
                             ) as s:
                data = monitor.get_system_data(system=test_system_id,
                                               session=s)
                # When the folder does not exist it should return empty df
                monitor.conf.set('DEFAULT', 'folder', 'do-not-exist')
                self.assertTrue(monitor.get_system_data(system=test_system_id,
                                                        session=s).empty)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)

    def test_getsyslogs(self):
        """ Test function for get_system_logs """
        test_system_id = 'System_1'
        with self.sandbox.collector as col:
            with SftpSession(
                hostname='127.0.0.1',
                logger=LOGGER,
                ssh_port=col.server.tunnelports[test_system_id]
            ) as s:
                logs = col.get_system_logs(
                           ssh_session=s.ssh_transport,
                           system=test_system_id,
                           log_cmd='netstat -nrt'
                       )
                self.assertIn('0.0.0.0', ''.join(logs))
                logs = col.get_system_logs(
                           ssh_session=s.get_channel(),
                           system=test_system_id
                       )
                self.assertIsNone(logs)

    def test_collectsysdata(self):
        """ Test function for collect_system_data """
        with self.sandbox.collector as monitor:
            monitor.alldays = True  # Ignore timestamp on test data
            monitor.nologs = False

            monitor.conf.set('MISC', 'remote_log_cmd', 'netstat -nrt')
            (data, logs) = monitor.collect_system_data(system='System_1')
            self.assertFalse(data.empty)
            self.assertNotIn('Log collection omitted', logs)

            monitor.nologs = True  # Skip log collection
            monitor.conf.set('DEFAULT', 'folder', '')
            (data, logs) = monitor.collect_system_data(system='System_1')
            self.assertIsInstance(data, pd.DataFrame)
            self.assertTrue(data.empty)  # sftp folder was not set
            self.assertIn('Log collection omitted', logs)

    def test_serialmain(self):
        """ Test function for main_no_threads (serial mode) """
        self.sandbox.collector.main_no_threads()
        self.assertIsInstance(self.sandbox.collector.data, pd.DataFrame)
        self.assertFalse(self.sandbox.collector.data.empty)
        self.assertIsInstance(self.sandbox.collector.logs, dict)

    def test_threadedmain(self):
        """ Test function for main_threads (threaded mode) """
        monitor = collector.Collector(settings_file=TEST_CONFIG,
                                      logger=LOGGER,
                                      alldays=True,
                                      nologs=True)
        monitor.conf.set('DEFAULT', 'folder', MY_DIR)
        monitor.main_threads()
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertFalse(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)

    def test_collector_start(self):
        """ Test function for Collector.start """
        monitor = collector.Collector(settings_file=TEST_CONFIG,
                                      logger=LOGGER,
                                      alldays=True,
                                      nologs=True)
        monitor.start()
        # main reads by itself the config file, where the folder is not set
        # thus won't find the files and return an empty dataframe
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertTrue(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)

        # Same by calling the threaded version
        monitor = collector.Collector(alldays=True,
                                      logger=LOGGER,
                                      nologs=True,
                                      settings_file=TEST_CONFIG,
                                      threaded=True)
        monitor.start()
        self.assertIsInstance(monitor.data, pd.DataFrame)
        self.assertTrue(monitor.data.empty)
        self.assertIsInstance(monitor.logs, dict)


if __name__ == "__main__":
    unittest.main()
