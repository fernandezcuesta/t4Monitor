#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import unittest
import tempfile

import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

from pysmscmon import smscmon as smsc
from pysmscmon import df_tools
from pysmscmon import logger

TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')
TEST_CSV = 'test/test_data.csv'
TEST_PKL = 'test/test_data.pkl'


class TestAuxiliaryFunctions(unittest.TestCase):
    """ Test auxiliary functions, only executed from CLI
        Moved to a separate class since it affects _metadata, which extends to
        all objects of the pandas.DataFrame class. Placed before TestDFTools
        because py.test runs tests in order of appearance
    """
    @classmethod
    def setUpClass(cls):
        smsc.add_methods_to_pandas_dataframe(LOGGER)

    def test_restore_plain_csv(self):
        """ Test function for auxiliary metadata_from_cols and reload_from_csv
        """
        # my_df = pd.read_pickle(TEST_PKL)
        with open(TEST_CSV, 'r') as testcsv:
            df1 = df_tools.to_dataframe(*df_tools.extract_t4csv(testcsv))
        with tempfile.NamedTemporaryFile() as plaincsv:
            df1.to_csv(plaincsv)
            plaincsv.file.close()

            df2 = df_tools.reload_from_csv(plaincsv.name)
        self.assertTupleEqual(df1.shape, df2.shape)
        self.assertListEqual(df1._metadata, df2._metadata)
        for meta_item in df1._metadata:
            self.assertEqual(getattr(df1, meta_item), getattr(df2, meta_item))


class TestDFTools(unittest.TestCase):
    """ Set of test functions for df_tools.py """
    def test_extract_t4csv(self):
        """ Test function for extract_t4csv """
        with open(TEST_CSV, 'r') as filedescriptor:
            fields, data, metadata = df_tools.extract_t4csv(filedescriptor)

        self.assertIsInstance(fields, list)
        self.assertIsInstance(data, list)
        self.assertEqual(len(fields), len(data[0].split(df_tools.SEPARATOR)))
        self.assertEquals(set([len(fields)]),
                          set([len(row.split(df_tools.SEPARATOR))
                               for row in data]))
        # Specific to this particular test file
        self.assertEqual(metadata['system'], 'SYSTEM1')
        self.assertIn('Counter07_HANDOVER_RQST', fields)
        self.assertIn('Sample Time', fields)
        self.assertIn('[DISK_BCK0]%Used', fields)
        self.assertIn('Counter01_message_External_Failure', fields)

    def test_select_var(self):
        """ Test function for select_var """
        dataframe = pd.read_pickle(TEST_PKL)

        self.assertListEqual(list(dataframe.columns),
                             df_tools.select_var(dataframe, '',
                                                 logger=LOGGER))
        self.assertListEqual(df_tools.select_var(dataframe,
                                                 'NONEXISTING_COLUMN',
                                                 logger=LOGGER),
                             [])
        self.assertListEqual(df_tools.select_var(dataframe,
                                                 'NONEXISTING_COLUMN',
                                                 system='no-system',
                                                 logger=LOGGER),
                             [])
        # Specific for test data
        self.assertEqual(len(df_tools.select_var(dataframe,
                                                 'Above_Peek',
                                                 logger=LOGGER)),
                         12)

        self.assertEqual(len(df_tools.select_var(dataframe,
                                                 'Counter0',
                                                 logger=LOGGER)),
                         370)

    def test_extractdf(self):
        """ Test function for extract_df """
        dataframe = pd.read_pickle(TEST_PKL)
        # Extract non existing -> empty
        self.assertTrue(df_tools.extract_df(dataframe,
                                            'NONEXISTING_COLUMN',
                                            logger=LOGGER).empty)
        # Extract none -> original
        assert_frame_equal(dataframe, df_tools.extract_df(dataframe,
                                                          '',
                                                          logger=LOGGER))
        # Extract none, filtering by a non-existing system
        assert_frame_equal(pd.DataFrame(), df_tools.extract_df(dataframe,
                                                               system='BAD_ID',
                                                               logger=LOGGER))
        # Extract filtering by an existing system (only one in this case)
        assert_frame_equal(dataframe,
                           df_tools.extract_df(dataframe,
                                               system='SYSTEM1',
                                               logger=LOGGER))
        # Extract an empty DF should return empty DF
        assert_frame_equal(pd.DataFrame(), df_tools.extract_df(pd.DataFrame(),
                                                               logger=LOGGER))

    def test_todataframe(self):
        """ Test function for to_dataframe """
        with open(TEST_CSV, 'r') as filedescriptor:
            header, data, metadata = df_tools.extract_t4csv(filedescriptor)
        dataframe = df_tools.to_dataframe(header, data, metadata)
        self.assertIsInstance(dataframe, pd.DataFrame)
        self.assertTupleEqual(dataframe.shape, (286, 931))
        # Missing header should return an empty DF
        self.assertTrue(df_tools.to_dataframe([], data, metadata).empty)
        # # Missing data should return an empty DF
        self.assertTrue(df_tools.to_dataframe(header, [], metadata).empty)
        # # Missing metadata should return metadata-ready empty DF
        # for item in dataframe._metadata:
        #     self.assertIn(item, df_tools.to_dataframe(header,
        #                                               data,
        #                                               {})._metadata)
        my_df = df_tools.to_dataframe(['COL1', 'My Sample Time'],
                                      ['7, 2000-01-01 00:00:01',
                                       '23, 2000-01-01 00:01:00',
                                       '30, 2000-01-01 00:01:58'], {})
        self.assertEqual(my_df['COL1'].sum(), 60)
        self.assertIsInstance(my_df.index, pd.DatetimeIndex)

    def test_metadata_copyrestore(self):
        """ Test function for copy_metadata() and restore_metadata()
        """
        my_df = df_tools.to_dataframe(['COL1', 'My Sample Time'],
                                      ['7, 2000-01-01 00:00:01',
                                       '23, 2000-01-01 00:01:00',
                                       '30, 2000-01-01 00:01:58'],
                                      {'addressing': 'LOCAL',
                                       'missing': 107,
                                       'fresh': False})
        metadata_bck = df_tools.copy_metadata(my_df)
        empty_df = pd.DataFrame()
        df_tools.restore_metadata(metadata_bck, empty_df)
        # Check that empty_df._metadata values are copied
        for item in my_df._metadata:
            self.assertIn(item, empty_df._metadata)

    def test_dataframize(self):
        """ Test function for dataframize """
        dataframe = df_tools.dataframize(TEST_CSV, logger=LOGGER)
        self.assertTupleEqual(dataframe.shape, (286, 931))
        self.assertTrue(hasattr(dataframe, '_metadata'))
        # Check that metadata is in place and extra columns were created too
        for item in dataframe._metadata:
            self.assertTrue(hasattr(dataframe, item))
            self.assertIn(item, dataframe)
        # test with a non-T4Format2 CSV, should return empty DF
        with tempfile.NamedTemporaryFile() as plaincsv:
            dataframe.to_csv(plaincsv)
            plaincsv.file.close()
            assert_frame_equal(pd.DataFrame(),
                               df_tools.dataframize(plaincsv.name))

        # test when file does not exist
        assert_frame_equal(pd.DataFrame(),
                           df_tools.dataframize('non-existing-file'))

    def test_compressed_pickle(self):
        """ Test to_pickle and read_pickle for compressed pkl.gz files """
        dataframe = df_tools.dataframize(TEST_CSV, logger=LOGGER)
        with tempfile.NamedTemporaryFile() as picklegz:
            dataframe.to_pickle(picklegz.name, compress=True)
            picklegz.file.close()
            picklegz.name = '{}.gz'.format(picklegz.name)
            assert_frame_equal(dataframe,
                               pd.read_pickle(picklegz.name,
                                              compress=True))

    def consolidate_data(self):
        """ Test for consolidate_data """
        dataframe = df_tools.dataframize(TEST_CSV, logger=LOGGER)
        assert_frame_equal(df_tools.consolidate_data(dataframe), dataframe)
        assert_frame_equal(df_tools.consolidate_data(dataframe, dataframe),
                           dataframe)
        assert_frame_equal(df_tools.consolidate_data(dataframe, pd.DataFrame()),
                           dataframe)
