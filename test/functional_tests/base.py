#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring base test functions for unit tests
"""
from __future__ import absolute_import

import shutil
import socket
import tempfile
import unittest
from os import path, remove

import paramiko
from six import print_

from t4mon.arguments import get_absolute_path
from ..unit_tests.base import BaseTestClass, MY_DIR, TEST_CONFIG

TEST_CONFIG = TEST_CONFIG


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
            print_('Exception while trying to setup interactive SSH tests. '
                   'It is assumed that SSH to localhost is setup with pkey '
                   'and settings are configured in ~/.ssh/config for '
                   '"localhost".\nERROR: {0}'.format(exc))
            raise unittest.SkipTest

    def setUp(self):  # make a clone for every new test
        # Specify where the test files are
        self.sandbox = self.orchestrator_test.clone()
        self.sandbox.collector = self.collector_test
        self.sandbox.collector.conf.set('DEFAULT', 'folder', MY_DIR)
        self.sandbox.__setattr__('conf', self.sandbox.collector.conf)


class TestWithTempConfig(TestWithSsh):

    """ Test class to handle working in a temporary directory """
    @classmethod
    def setUpClass(cls):
        super(TestWithTempConfig, cls).setUpClass()
        cls.conf = cls.collector_test.conf
        # read_config(settings_file=TEST_CONFIG)
        cls.temporary_dir = tempfile.gettempdir()
        cls.orchestrator_test.logger.info('Using temporary directory: {0}'
                                          .format(cls.temporary_dir))

        # Copy all required files to the temporary directory
        for misc_item in ['calculations_file',
                          'html_template',
                          'graphs_definition_file']:
            _file = get_absolute_path(cls.conf.get('MISC', misc_item),
                                      cls.orchestrator_test.settings_file)
            cls.orchestrator_test.logger.debug('Copying {0} to {1}'
                                               .format(_file,
                                                       cls.temporary_dir))
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
                cls.orchestrator_test.logger.debug('Deleting {0}'
                                                   .format(ext_file_path))
                remove(ext_file_path)

    def setUp(self):
        super(TestWithTempConfig, self).setUp()
