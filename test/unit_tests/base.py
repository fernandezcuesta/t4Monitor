#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring base test functions for functional tests
"""

import Queue
import shutil
import unittest
from os import path

import numpy as np
import pandas as pd

from t4mon import logger
from t4mon.collector import Collector, add_methods_to_pandas_dataframe
from t4mon.orchestrator import Orchestrator

__all__ = ('BaseTestClass',
           'OrchestratorSandbox',
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


class OrchestratorSandbox(Orchestrator):

    def clone(self, system=''):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
            This method is ONLY used in test functions.
        """
        my_clone = Orchestrator(logger=self.logger)
        my_clone.calculations_file = self.calculations_file
        my_clone.collector = self.collector
        my_clone.data = self.data
        my_clone.date_time = self.date_time
        my_clone.graphs_definition_file = self.graphs_definition_file
        my_clone.html_template = self.html_template
        if system in self.logs:
            my_clone.logs[system] = self.logs[system]
        my_clone.reports_written = self.reports_written
        my_clone.reports_folder = self.reports_folder
        my_clone.store_folder = self.store_folder
        my_clone.safe = self.safe

        return my_clone


class CollectorSandbox(Collector):

    def clone(self):
        """ Makes a copy of a Collector object
        """
        my_clone = Collector()
        my_clone.alldays = self.alldays
        my_clone.conf = self.conf
        my_clone.data = self.data.copy()  # required in pandas
        my_clone.logger = self.logger
        my_clone.logs = self.logs
        my_clone.nologs = self.nologs
        my_clone.results_queue = Queue.Queue()  # make a brand new queue
        my_clone.server = self.server
        my_clone.settings_file = self.settings_file
        return my_clone


class BaseTestClass(unittest.TestCase):

    """ Base TestCase for unit and functional tests """
    @classmethod
    def setUpClass(cls):
        cls.collector_test = CollectorSandbox(logger=LOGGER,
                                              settings_file=TEST_CONFIG)
        cls.orchestrator_test = OrchestratorSandbox(logger=LOGGER,
                                                    settings_file=TEST_CONFIG)
        cls.orchestrator_test.logs['my_sys'] = 'These are my dummy log results'
        add_methods_to_pandas_dataframe(LOGGER)

    @classmethod
    def tearDownClass(cls):
        for folder in [cls.orchestrator_test.reports_folder,
                       cls.orchestrator_test.store_folder]:
            if path.isdir(folder):
                try:
                    shutil.rmtree(folder)
                    cls.orchestrator_test.logger.debug(
                        'Temporary folder deleted: %s',
                        folder
                    )
                except OSError:
                    pass  # was already deleted or no permissions
