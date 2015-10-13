import socket
import unittest
from os import path, sep

import numpy as np
import pandas as pd
import paramiko

from pysmscmon import smscmon as smsc
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
MY_DIR = path.dirname(path.abspath(__file__))


class TestWithSsh(unittest.TestCase):
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
