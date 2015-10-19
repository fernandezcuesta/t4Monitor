#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import

import shutil
import socket
import tempfile
import unittest
from os import path, remove

import paramiko

from t4mon.collector import read_config, add_methods_to_pandas_dataframe

from ..unit_tests.base import *


class TestWithSsh(BaseTestClass):
    """ Set of test functions for interactive (ssh) methods of collector.py """
    @classmethod
    def setUpClass(cls):
        super(TestWithSsh, cls).setUpClass()
        add_methods_to_pandas_dataframe(LOGGER)
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


class TestWithTempConfig(TestWithSsh):
    """ Test class to handle working in a temporary dir """
    @classmethod
    def setUpClass(cls):
        super(TestWithTempConfig, cls).setUpClass()
        cls.conf = read_config(settings_file=TEST_CONFIG)
        cls.temporary_dir = tempfile.gettempdir()
        cls.orchestrator.logger.info('Using temporary dir: %s',
                                  cls.temporary_dir)

        calcs_file = cls.orchestrator.get_absolute_path(
            cls.conf.get('MISC', 'calculations_file'))
        shutil.copy(calcs_file,
                    cls.temporary_dir)

        html_template = cls.orchestrator.get_absolute_path(
            cls.conf.get('MISC', 'html_template'))
        shutil.copy(html_template,
                    cls.temporary_dir)

        graphs_file = cls.orchestrator.get_absolute_path(
            cls.conf.get('MISC', 'graphs_definition_file'))
        shutil.copy(graphs_file,
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
                remove(ext_file_path)
