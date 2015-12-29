#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring base test functions for unit tests
"""
from __future__ import print_function, absolute_import

import shutil
import socket
import tempfile
import unittest

from os import path, remove

import paramiko

from t4mon.collector import read_config

from ..unit_tests.base import *


class TestWithSsh(BaseTestClass):

    """ Set of test functions for interactive (ssh) methods of collector.py """
    @classmethod
    def setUpClass(cls):
        super(TestWithSsh, cls).setUpClass()
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

    def setUp(self):  # make a clone for every new test
        # Specify where the test files are
        self.sandbox = self.orchestrator_test.clone()
        self.sandbox.collector = self.collector_test
        self.sandbox.collector.conf.set('DEFAULT', 'folder', MY_DIR)
        self.sandbox.__setattr__('conf', self.sandbox.collector.conf)


class TestWithTempConfig(TestWithSsh):

    """ Test class to handle working in a temporary dir """
    @classmethod
    def setUpClass(cls):
        super(TestWithTempConfig, cls).setUpClass()
        cls.conf = read_config(settings_file=TEST_CONFIG)  # TODO: remove this?
        cls.temporary_dir = tempfile.gettempdir()
        cls.orchestrator_test.logger.info('Using temporary dir: %s',
                                          cls.temporary_dir)

        # Copy all required files to the temporary directory
        for misc_item in ['calculations_file',
                          'html_template',
                          'graphs_definition_file']:
            _file = cls.orchestrator_test.get_absolute_path(
                cls.conf.get('MISC', misc_item)
            )
            cls.orchestrator_test.logger.debug('Copying %s to %s',
                                               _file,
                                               cls.temporary_dir)
            shutil.copy(_file,
                        cls.temporary_dir)

    @classmethod
    def tearDownClass(cls):
        # Remove other temporary files
        super(TestWithTempConfig, cls).tearDownClass()
        for ext_file in ['calculations_file',
                         'html_template',
                         'graphs_definition_file']:
            ext_file_path = path.join(cls.temporary_dir,
                                      cls.conf.get('MISC', ext_file))
            if path.isfile(ext_file_path):
                cls.orchestrator_test.logger.debug('Deleting %s',
                                                   ext_file_path)
                remove(ext_file_path)

    def setUp(self):
        super(TestWithTempConfig, self).setUp()
