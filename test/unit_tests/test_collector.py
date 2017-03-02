#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for collector.py
"""
from __future__ import absolute_import

import logging
import datetime as dt
import tempfile

import pandas as pd
from t4mon import df_tools, arguments, collector
from six.moves import queue, configparser
from pandas.util.testing import assert_frame_equal

from .base import TEST_CSV, TEST_PKL, TEST_CALC, TEST_ZIPFILE, BaseTestClass


class TestCollector(BaseTestClass):

    """ Set of test functions for collector.py """

    def test_getstats(self):
        """ Test function for get_stats_from_host """
        df1 = self.collector_test.get_stats_from_host(filespec_list=TEST_CSV)
        col = collector.read_pickle(TEST_PKL)
        df2 = col.data
        df2.clean_calcs(TEST_CALC)  # undo calculations
        df1 = df_tools.consolidate_data(
            df1,
            system=df2.index.get_level_values('system').unique()[0]
        )

        self.assertIsInstance(df1, pd.DataFrame)
        self.assertIsInstance(df2, pd.DataFrame)
        assert_frame_equal(df1, df2)

    def test_collector_class(self):
        """ Test methods related to the Collector class """
        # first of all, check default values
        my_collector = collector.Collector()
        self.assertIsNone(my_collector.server)
        self.assertIsInstance(my_collector.results_queue, queue.Queue)
        self.assertIsInstance(my_collector.conf,
                              configparser.ConfigParser)
        self.assertFalse(my_collector.alldays)
        self.assertFalse(my_collector.nologs)
        self.assertFalse(my_collector.safe)
        self.assertIsInstance(my_collector.logger,
                              logging.Logger)
        self.assertEqual(my_collector.settings_file,
                         arguments.DEFAULT_SETTINGS_FILE)
        self.assertIsInstance(my_collector.data,
                              pd.DataFrame)
        self.assertDictEqual(my_collector.logs,
                             {})
        self.assertDictEqual(my_collector.filecache,
                             {})

        # Test also CollectorSandbox class methods
        coll_clone = self.collector_test.clone()
        coll_clone.alldays = coll_clone.nologs = False
        self.assertNotEqual(self.collector_test.alldays,
                            coll_clone.alldays)
        self.assertNotEqual(self.collector_test.nologs,
                            coll_clone.nologs)

        self.assertEqual(self.collector_test.logger,
                         coll_clone.logger)
        self.assertEqual(self.collector_test.settings_file,
                         coll_clone.settings_file)
        self.assertEqual(self.collector_test.server,
                         coll_clone.server)
        self.assertListEqual(self.collector_test.conf.sections(),
                             coll_clone.conf.sections())
        for section in coll_clone.conf.sections():
            self.assertListEqual(self.collector_test.conf.items(section),
                                 coll_clone.conf.items(section))
        self.assertEqual(self.collector_test.safe,
                         coll_clone.safe)
        self.assertDictEqual(self.collector_test.filecache,
                             coll_clone.filecache)

        self.assertIn('Settings file: {0}'.format(
                      self.collector_test.settings_file
                      ),
                      self.collector_test.__str__())

    def test_compressed_pickle(self):
        """ Test to_pickle and read_pickle for compressed pkl.gz files """
        self.logger.error(self.collector_test.__dict__)
        with tempfile.NamedTemporaryFile() as picklegz:
            self.collector_test.to_pickle(name=picklegz.name,
                                          compress=True)
            picklegz.file.close()
            picklegz.name = '{0}.gz'.format(picklegz.name)
            assert_frame_equal(self.collector_test.data,
                               collector.read_pickle(picklegz.name,
                                                     compress=True).data)
            # We should be able to know this is a compressed pickle just by
            # looking at the .gz extension
            self.collector_test.to_pickle(name=picklegz.name)
            picklegz.file.close()
            assert_frame_equal(self.collector_test.data,
                               collector.read_pickle(picklegz.name).data)

            # Uncompressed still works ;)
            self.collector_test.to_pickle(picklegz.name.rstrip('.gz'))
            assert_frame_equal(self.collector_test.data,
                               collector.read_pickle(picklegz.name).data)

    def test_load_zipfile(self):
        """ Test function for load_zipfile """
        _df = collector.load_zipfile(TEST_ZIPFILE, system='CSV')
        self.assertIsInstance(_df, pd.DataFrame)
        self.assertTupleEqual(_df.shape, (2876, 787))
        # Bad zipfile should return an empty dataframe
        _df = collector.load_zipfile(TEST_PKL)
        self.assertTrue(_df.empty)

    def test_get_datetag(self):
        """ Test function for get_datetag """
        today = dt.datetime.today()
        other = dt.datetime.strptime('01/01/12', '%d/%m/%y')
        # Running without arguments should return current date
        self.assertEqual(collector.get_datetag(),
                         collector.get_datetag(today))
        self.assertEqual(collector.get_datetag(other),
                         '01jan2012')
