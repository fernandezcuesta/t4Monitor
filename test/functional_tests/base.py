#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import socket
import unittest

import paramiko

from pysmscmon.smscmon import add_methods_to_pandas_dataframe

from ..unit_tests.base import *


class TestWithSsh(unittest.TestCase):
    """ Set of test functions for interactive (ssh) methods of smscmon.py """
    @classmethod
    def setUpClass(cls):
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
