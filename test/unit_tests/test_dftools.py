#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for df_tools.py
"""
from __future__ import absolute_import

import tempfile
import unittest

import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from t4mon import df_tools, collector

from .base import LOGGER, TEST_CSV, BaseTestClass

TEST_CSV_SHAPE = (286, 930)  # dataframe shape as generated from CSV
TEST_PKL_SHAPE = (286, 942)  # dataframe shape after calculations (as stored)
TEST_PLAINCSV = 'test/plain_csv.txt'


class TestAuxiliaryFunctions(unittest.TestCase):

    """ Test auxiliary functions, do not require setting anything up
    """
    @classmethod
    def setUpClass(cls):
        collector.add_methods_to_pandas_dataframe(LOGGER)

    def test_reload_from_csv(self):
        """ Test loading a dataframe from CSV file (plain or T4 format)
        """
        # Test with a T4-CSV
        df1 = df_tools.reload_from_csv(TEST_CSV)
        self.assertTupleEqual(df1.shape, TEST_CSV_SHAPE)
        # Test with a plain CSV
        with tempfile.NamedTemporaryFile() as plaincsv:
            df1.to_csv(plaincsv)
            plaincsv.file.close()
            df2 = df_tools.reload_from_csv(plaincsv.name, plain=True)
            assert_frame_equal(df1, df2)

    def test_t4csv_to_plain(self):
        """ Test T4 to plain CSV conversion """
        with tempfile.NamedTemporaryFile() as test_plain:
            df_tools.t4csv_to_plain(TEST_CSV, output=test_plain)
            with open(TEST_PLAINCSV, 'r') as this, open(test_plain.name,
                                                        'r') as that:
                self.assertTrue(this.read(), that.read())

    def test_plain_to_t4csv(self):
        """ Test conversion from plain to T4-CSV """
        with tempfile.NamedTemporaryFile() as that:
            df_tools.plain_to_t4csv(TEST_PLAINCSV, output=that.name)
            with open(TEST_PLAINCSV, 'r') as this:
                self.assertTrue(this.read(), that.read())

    @pytest.mark.xfail(reason='Statistically possible to fail, random numbers')
    def test_remove_outliers(self):
        """ Test removing outliers from a dataframe """
        df1 = pd.DataFrame(np.random.randn(8, 4), columns=['A', 'B', 'C', 'D'])
        df2 = df1.copy()
        # Add some outliers in random positions
        a = range(len(df2))
        np.random.shuffle(a)
        rnd_row = a[:1]
        a = df2.columns.values
        rnd_col = a[:3]
        df2.update(df2[rnd_col].ix[rnd_row] * 99.0)
        df2 = df_tools.remove_outliers(df2, n_std=2)
        assert_frame_equal(df1.drop(rnd_row, axis=0), df2)


class TestDFTools(BaseTestClass):

    """ Set of test functions for df_tools.py """

    def test_extract_t4csv(self):
        """ Test function for extract_t4csv """
        with open(TEST_CSV, 'r') as filedescriptor:
            (fields, data) = df_tools.extract_t4csv(filedescriptor)

        self.assertIsInstance(fields, list)
        self.assertIsInstance(data, list)
        self.assertEqual(len(fields), len(data[0].split(df_tools.SEPARATOR)))
        self.assertEquals(set([len(fields)]),
                          set([len(row.split(df_tools.SEPARATOR))
                               for row in data]))
        # Specific to this particular test file
        self.assertIn('Counter07_HANDOVER_RQST', fields)
        self.assertIn('Sample Time', fields)
        self.assertIn('[DISK_BCK0]%Used', fields)
        self.assertIn('Counter01_message_External_Failure', fields)

    def test_select(self):
        """ Test function for select """

        # Extract non existing -> empty
        self.assertTrue(df_tools.select(self.test_data,
                                        'NONEXISTING_COLUMN',
                                        logger=self.logger).empty)
        # Extract none -> original
        assert_frame_equal(self.test_data, df_tools.select(self.test_data,
                                                           '',
                                                           logger=self.logger))
        # Extract none, filtering by a non-existing system
        assert_frame_equal(pd.DataFrame(), df_tools.select(self.test_data,
                                                           system='BAD_ID',
                                                           logger=self.logger))
        # Extract filtering by an existing system (only one in this case)
        self.assertTupleEqual(df_tools.select(self.test_data,
                                              system='SYSTEM_1',
                                              logger=self.logger).shape,
                              TEST_PKL_SHAPE)  # calcs applied, 930->944
        # Extract an empty DF should return empty DF
        assert_frame_equal(pd.DataFrame(), df_tools.select(pd.DataFrame(),
                                                           logger=self.logger))
        # Specific for test data
        self.assertEqual(df_tools.select(self.test_data,
                                         'Above_Peek',
                                         logger=self.logger).shape[1],
                         12)

        self.assertEqual(df_tools.select(self.test_data,
                                         'Counter0',
                                         logger=self.logger).shape[1],
                         382)
        # Bad additional filter returns empty dataframe
        assert_frame_equal(df_tools.select(self.test_data,
                                           'Above_Peek',
                                           position='UP',  # wrong filter
                                           logger=self.logger),
                           pd.DataFrame())
        # When a wrong variable is selected, it is ignored
        self.assertEqual(df_tools.select(self.test_data,
                                         'I_do_not_exist',
                                         'Above_Peek',
                                         logger=self.logger).shape[1],
                         12)

    def test_todataframe(self):
        """ Test function for to_dataframe """
        with open(TEST_CSV, 'r') as testcsv:
            (field_names, data) = df_tools.extract_t4csv(testcsv)
        dataframe = df_tools.to_dataframe(field_names, data)
        self.assertIsInstance(dataframe, pd.DataFrame)
        self.assertTupleEqual(dataframe.shape, TEST_CSV_SHAPE)
        # Missing header should return an empty DF
        self.assertTrue(df_tools.to_dataframe([], data).empty)
        # # Missing data should return an empty DF
        self.assertTrue(df_tools.to_dataframe(field_names, []).empty)
        my_df = df_tools.to_dataframe(['COL1', 'My Sample Time'],
                                      ['7, 2000-01-01 00:00:01',
                                       '23, 2000-01-01 00:01:00',
                                       '30, 2000-01-01 00:01:58'])
        self.assertEqual(my_df['COL1'].sum(), 60)
        self.assertIsInstance(my_df.index, pd.DatetimeIndex)

    def test_todataframe_raises_exception_if_no_datetime_column_found(self):
        """
        Test to_dataframe when a no header passed matching the datetime tag
        """
        with open(TEST_CSV, 'r') as testcsv:
            (field_names, data) = df_tools.extract_t4csv(testcsv)
        # fake the header
        df_timecol = (s for s in field_names
                      if df_tools.DATETIME_TAG in s).next()
        field_names[field_names.index(df_timecol)] = 'time_index'
        with self.assertRaises(df_tools.ToDfError):
            df_tools.to_dataframe(field_names, data)

    def test_dataframize(self):
        """ Test function for dataframize """
        dataframe = df_tools.dataframize(TEST_CSV, logger=self.logger)
        self.assertTupleEqual(dataframe.shape, TEST_CSV_SHAPE)
        # test with a non-T4Format CSV, should return empty DF
        with tempfile.NamedTemporaryFile() as plaincsv:
            dataframe.to_csv(plaincsv)
            plaincsv.file.close()
            assert_frame_equal(pd.DataFrame(),
                               df_tools.dataframize(plaincsv.name))
        # test when file does not exist
        assert_frame_equal(pd.DataFrame(),
                           df_tools.dataframize('non-existing-file'))

    def test_consolidate_data(self):
        """ Test dataframe consolidation function """
        midx = pd.MultiIndex(levels=[[0, 1, 2, 3, 4], ['sys1']],
                             labels=[[0, 1, 2, 3, 4], [0, 0, 0, 0, 0]],
                             names=[df_tools.DATETIME_TAG, 'system'])

        df1 = pd.DataFrame(np.random.randint(0, 10, (5, 3)),
                           columns=['A', 'B', 'C'])
        df1.index.name = df_tools.DATETIME_TAG

        df1_midx = df1.set_index(midx)

        df2 = pd.DataFrame(np.random.randint(0, 10, (5, 3)),
                           columns=['A', 'B', 'C'])
        df2.index.name = df_tools.DATETIME_TAG

        # Consolidate with nothing should raise a ToDfError
        with self.assertRaises(df_tools.ToDfError):
            df_tools.consolidate_data(df1)

        # Consolidate with a system name should return MultiIndex dataframe
        assert_frame_equal(df_tools.consolidate_data(df1.copy(),
                                                     system='sys1'),
                           df1_midx)

        data = pd.DataFrame()
        for (i, partial_dataframe) in enumerate([df1, df2]):
            data = df_tools.consolidate_data(partial_dataframe,
                                             dataframe=data,
                                             system='sys{}'.format(i + 1))
        self.assertTupleEqual(data.shape, (10, 3))
        self.assertTupleEqual(data.index.levshape, (5, 2))

        assert_frame_equal(df1, data.xs('sys1', level='system'))
        assert_frame_equal(df2, data.xs('sys2', level='system'))

    def test_dataframe_to_t4csv(self):
        """ Test reverse conversion (dataframe to T4-CSV) """

        t = tempfile.NamedTemporaryFile()
        t4files = df_tools.dataframe_to_t4csv(dataframe=self.test_data,
                                              output=t.name)
        self.assertTrue(len(t4files) > 0)
        that = self.collector_test.get_stats_from_host(t4files.values())
        that = df_tools.consolidate_data(that, system=t4files.keys()[0])
        assert_frame_equal(self.test_data, that)

    def test_remove_duplicate_columns(self):
        """ Test removing duplicate columns from a dataframe """
        df1 = pd.DataFrame(np.random.randint(0, 10, (5, 5)),
                           columns=list('ABCBE'))
        df2 = df_tools.remove_duplicate_columns(df1)
        self.assertTupleEqual(df2.shape, (5, 4))
        # When no duplicates should not alter the dataframe
        assert_frame_equal(df2, df_tools.remove_duplicate_columns(df2))

    def test_get_matching_columns(self):
        """
        Test getting a list of columns based on regex and exclusion lists
        """
        df1 = pd.DataFrame(np.random.randint(0, 10, (2, 9)),
                           columns=['one', 'two', 'one two', 'four',
                                    'five', 'six', 'one six', 'eight',
                                    'four five'])
        self.assertEqual(['one', 'one two'],
                         df_tools.get_matching_columns(df1, 'one',
                                                       excluded='six'))
