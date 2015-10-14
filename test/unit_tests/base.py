#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import socket
import unittest
from os import sep, path

import numpy as np
import pandas as pd
import paramiko

from pysmscmon import smscmon as smsc
from pysmscmon import logger

__all__ = ('MY_DIR',
           'LOGGER',
           'TEST_CONFIG',
           'BAD_CONFIG',
           'TEST_CSV',
           'TEST_DATAFRAME',
           'TEST_GRAPHS_FILE',
           'TEST_HTMLTEMPLATE',
           'TEST_PKL')

MY_DIR = path.dirname(path.abspath(__file__))
LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')

TEST_CONFIG = 'test/test_settings.cfg'
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
