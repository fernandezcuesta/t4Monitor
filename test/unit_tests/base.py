#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import shutil
import socket
import unittest
from os import path

import numpy as np
import pandas as pd
import paramiko

from t4mon.orchestrator import Orchestrator
from t4mon import logger

__all__ = ('BaseTestClass',
           'MY_DIR',
           'LOGGER',
           'TEST_CONFIG',
           'BAD_CONFIG',
           'TEST_CSV',
           'TEST_DATAFRAME',
           'TEST_GRAPHS_FILE',
           'TEST_HTMLTEMPLATE',
           'TEST_PKL')

LOGGER = logger.init_logger(loglevel='DEBUG', name='test-t4mon')

TEST_CONFIG = 'test/test_settings.cfg'
MY_DIR = path.dirname(path.abspath(TEST_CONFIG))
BAD_CONFIG = 'test/test_settings_BAD.cfg'
TEST_CSV = 'test/test_data.csv'
TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
TEST_GRAPHS_FILE = 'test/test_graphs.cfg'
TEST_HTMLTEMPLATE = 'test/test_template.html'
TEST_PKL = 'test/test_data.pkl'


class BaseTestClass(unittest.TestCase):
    """ Base TestCase for unit and functional tests """

    @classmethod
    def setUpClass(cls):
        cls.orchestrator = Orchestrator(logger=LOGGER,
                                        settings_file=TEST_CONFIG)
        cls.orchestrator.logs['my_sys'] = 'These are my dummy log results'

    @classmethod
    def tearDownClass(cls):
        for folder in [cls.orchestrator.reports_folder,
                       cls.orchestrator.store_folder]:
            if path.isdir(folder):
                try:
                    shutil.rmtree(folder)
                    cls.orchestrator.logger.debug('Temporary folder deleted: %s',
                                               folder)
                except OSError:
                    pass  # was already deleted or no permissions
