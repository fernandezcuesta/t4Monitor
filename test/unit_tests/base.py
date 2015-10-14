#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import shutil
import socket
import unittest
from os import path

import numpy as np
import pandas as pd
import paramiko

import pysmscmon
from pysmscmon import smscmon as smsc
from pysmscmon import logger

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

LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')

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
        cls.container = pysmscmon.Container(logger=LOGGER,
                                            settings_file=TEST_CONFIG)

    @classmethod
    def tearDownClass(cls):
        for folder in [cls.container.reports_folder,
                       cls.container.store_folder]:
            if path.isdir(folder):
                cls.container.logger.debug('Deleting temporary folder: %s',
                                           folder)
                shutil.rmtree(folder)
