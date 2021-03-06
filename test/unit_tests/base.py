#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring base test functions for functional tests
"""

import sys
import pickle
import shutil
import logging
import tempfile
import unittest
from os import path

import numpy as np
import pandas as pd
from t4mon import logger
from t4mon.arguments import read_config
from t4mon.collector import (
    Collector,
    read_pickle,
    add_methods_to_pandas_dataframe
)
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
TEST_CALC = 'test/test_calc.cfg'
TEST_CSV = 'test/test_data.csv'
TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
TEST_GRAPHS_FILE = 'test/test_graphs.cfg'
TEST_HTMLTEMPLATE = 'test/test_template.html'
TEST_ZIPFILE = 'test/test_t4.zip'
TEST_PKL = 'test/test_data{}.pkl.gz'.format(sys.version_info[0])


def random_tag(n=5):
    """ Return a n-digit string for random file naming """
    return ''.join(str(l) for l in np.random.randint(1, 10, n))


def delete_temporary_folder(folder, logger=None):
    """
    Delete a temporary folder in the local file system
    """
    if path.isdir(folder):
        try:
            shutil.rmtree(folder)
            if logger:
                logger.debug('Temporary folder deleted: {0}'.format(folder))
        except OSError:
            pass  # was already deleted or no permissions


class MockLoggingHandler(logging.Handler, object):
    """Mock logging handler to check for expected logs.

    Messages are available from an instance's `messages` dict, in order,
    indexed by a lowercase log level string (e.g., 'debug', 'info', etc.).
    """

    def __init__(self, *args, **kwargs):
        self.messages = {'debug': [], 'info': [], 'warning': [], 'error': [],
                         'critical': []}
        super(MockLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        "Store a message from ``record`` in the instance's ``messages`` dict."
        self.acquire()
        try:
            self.messages[record.levelname.lower()].append(record.getMessage())
        finally:
            self.release()

    def reset(self):
        self.acquire()
        try:
            for message_list in self.messages:
                self.messages[message_list] = []
        finally:
            self.release()


class OrchestratorSandbox(Orchestrator):

    def clone(self):
        """ Makes a copy of the data container where the system is filled in,
            data is shared with the original (note in pandas we need to do a
            pandas.DataFrame.copy(), otherwise it's just a view), date_time is
            copied from the original and logs and graphs are left unmodified.
            This method is ONLY used in test functions.
        """
        my_clone = OrchestratorSandbox(logger=self.logger,
                                       noreports=self.noreports,
                                       safe=self.safe,
                                       settings_file=self.settings_file,
                                       **self.kwargs.copy())
        # my_clone.collector = self.collector.clone()
        my_clone.data = self.data.copy()  # TODO: Is a copy really needed?
        my_clone.date_time = self.date_time
        my_clone.logs = self.logs.copy()
        my_clone.reports_written = []  # empty the written reports list
        my_clone.reports_folder = self.reports_folder + random_tag()
        my_clone.store_folder = self.store_folder + random_tag()
        my_clone.systems = self.systems[:]
        # store all the folders being created in a list, so we can delete them
        # all during teardown
        my_clone.folders = self.folders
        my_clone.folders.append(my_clone.reports_folder)
        my_clone.folders.append(my_clone.store_folder)
        my_clone._check_folders()  # force creation of destination folders

        return my_clone

    def __init__(self, *args, **kwargs):
        super(OrchestratorSandbox, self).__init__(*args, **kwargs)
        # Get external files from configuration
        self._check_external_files_from_config()
        self.folders = []

        # Override destination folders to be inside tempdir
        conf = read_config(self.settings_file)
        for folder in ['reports_folder', 'store_folder']:
            # first of all delete the original folders created during __init__
            conf_folder = getattr(self, folder)
            if conf_folder:
                delete_temporary_folder(conf_folder)
            value = conf.get('MISC', folder)
            setattr(self,
                    folder,
                    path.join(tempfile.gettempdir(), value))
            self.folders.append(folder)


class CollectorSandbox(Collector):

    def clone(self, system=''):
        """ Makes a copy of a Collector object
        """
        my_clone = CollectorSandbox(alldays=self.alldays,
                                    logger=self.logger,
                                    nologs=self.nologs,
                                    safe=self.safe,
                                    settings_file=self.settings_file)
        my_clone.conf = pickle.loads(pickle.dumps(self.conf))
        my_clone.data = self.data.copy()  # call by reference
        if system in self.logs:
            my_clone.logs[system] = self.logs[system]
        else:
            my_clone.logs = {}
        return my_clone


class BaseTestClass(unittest.TestCase):

    """ Base TestCase for unit and functional tests """
    @classmethod
    def setUpClass(cls):
        cls.logger = LOGGER
        cls._test_log_handler = MockLoggingHandler(level='DEBUG')
        cls.logger.addHandler(cls._test_log_handler)
        cls.test_log_messages = cls._test_log_handler.messages

        add_methods_to_pandas_dataframe(logger=cls.logger)
        cls.test_data = read_pickle(name=TEST_PKL, logger=cls.logger).data

        cls.collector_test = CollectorSandbox(logger=cls.logger,
                                              settings_file=TEST_CONFIG,
                                              nologs=True,
                                              alldays=True)
        cls.collector_test.logs['my_sys'] = 'These are my dummy log results'
        cls.collector_test.conf.set('DEFAULT', 'folder', MY_DIR)
        cls.orchestrator_test = OrchestratorSandbox(logger=cls.logger,
                                                    settings_file=TEST_CONFIG,
                                                    alldays=True)
        cls.orchestrator_test.logs = cls.collector_test.logs

    @classmethod
    def tearDownClass(cls):
        for folder in cls.orchestrator_test.folders:
            delete_temporary_folder(folder)
