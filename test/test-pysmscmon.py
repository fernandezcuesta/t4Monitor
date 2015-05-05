#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""

import unittest
import pysmscmon as smsc
import ConfigParser
import pandas as pd
import numpy as np
import logging
from pandas.util.testing import assert_frame_equal

TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
LOGGER = smsc.init_logger(loglevel='DEBUG', name='test-pysmscmon')
TEST_CSV = 'test_data.csv'
TEST_PKL = 'test_data.pkl'
TEST_CONFIG = 'test_settings.cfg'

class TestpySMSCmon(unittest.TestCase):
    """ Set of test functions for pySMSCMon module """
    
    def test_config(self):
        """ test function for read_config """
        config = smsc.read_config(TEST_CONFIG)
        self.assertIsInstance(config, ConfigParser.SafeConfigParser)
        self.assertGreater(config.sections(), 2)
        self.assertIn('GATEWAY', config.sections())
        self.assertTrue(all([key in [i[0] for i in config.items('DEFAULT')]
                             for key in ['ssh_port', 'ssh_timeout',
                                         'username', 'tunnel_port',
                                         'folder', 'ip_or_hostname']]))

    def test_extract_t4csv(self):
        """ Test function for extract_t4csv """
        with open(TEST_CSV, 'r') as filedescriptor:
            header, data_lines, metadata = smsc.extract_t4csv(filedescriptor)

        self.assertIsInstance(header, list)
        self.assertIsInstance(data_lines, list)
        self.assertEqual(len(header), len(data_lines[0].split(smsc.SEPARATOR)))
        self.assertEquals(set([len(header)]),
                          set([len(row.split(smsc.SEPARATOR))
                               for row in data_lines]))
        # Specific to this particular test file
        self.assertEqual(metadata['system'], 'SYSTEM1')
        self.assertIn('Counter07_HANDOVER_RQST', header)
        self.assertIn('Sample Time', header)
        self.assertIn('[DISK_BCK0]%Used', header)
        self.assertIn('Counter01_message_External_Failure', header)

    def test_tobase64(self):
        """ Test function for to_base64 """
        plot_fig = TEST_DATAFRAME.plot()
        self.assertIsInstance(smsc.to_base64(plot_fig), str)
        self.assertTrue(smsc.to_base64(plot_fig).
                        startswith('data:image/png;base64,'))

    def test_select_var(self):
        """ Test function for select_var """
        dataframe = pd.read_pickle(TEST_PKL)

        self.assertListEqual(list(dataframe.columns),
                             list(*smsc.select_var(dataframe, '',
                                                   logger=LOGGER)))
        self.assertListEqual(list(*smsc.select_var(dataframe,
                                                   'NONEXISTING_COLUMN',
                                                   logger=LOGGER)),
                             [])
        self.assertListEqual(list(*smsc.select_var(dataframe,
                                                   'NONEXISTING_COLUMN',
                                                   system='no-system',
                                                   logger=LOGGER)),
                             [])
        # Specific for test data
        self.assertEqual(len(list(*smsc.select_var(dataframe,
                                                   'Above_Peek',
                                                   logger=LOGGER))),
                         12)

        self.assertEqual(len(list(*smsc.select_var(dataframe,
                                                   'Counter0',
                                                   logger=LOGGER))),
                         370)

    def test_extractdf(self):
        """ Test function for extract_df """
        dataframe = pd.read_pickle(TEST_PKL)
        # Extract non existing -> empty
        self.assertTrue(smsc.extract_df(dataframe,
                                        'NONEXISTING_COLUMN',
                                        logger=LOGGER).empty)
        # Extract none -> original
        cosa = smsc.extract_df(dataframe, '', logger=LOGGER)
        LOGGER.info("%s/%s", cosa.shape, dataframe.shape)
        assert_frame_equal(dataframe, smsc.extract_df(dataframe,
                                                      '',
                                                      logger=LOGGER))

    def test_todataframe(self):
        """ Test function for to_dataframe """
        with open(TEST_CSV, 'r') as filedescriptor:
            header, data_lines, metadata = smsc.extract_t4csv(filedescriptor)
        dataframe = smsc.to_dataframe(header, data_lines, metadata)
        self.assertIsInstance(dataframe, pd.DataFrame)
        self.assertTupleEqual(dataframe.shape, (286, 931))
        # Missing header should return an empty DF
        self.assertTrue(smsc.to_dataframe([], data_lines, metadata).empty)
        # # Missing data should return an empty DF
        self.assertTrue(smsc.to_dataframe(header, [], metadata).empty)
        # # Missing metadata should return metadata-ready empty DF
        for item in dataframe._metadata:
            self.assertIn(item, smsc.to_dataframe(header,
                                                  data_lines,
                                                  {})._metadata)
        my_df = smsc.to_dataframe(['COL1', 'My Sample Time'],
                                  ['7, 2000-01-01 00:00:01',
                                   '23, 2000-01-01 00:01:00',
                                   '30, 2000-01-01 00:01:58'], {})
        self.assertEqual(my_df['COL1'].sum(), 60)
        self.assertIsInstance(my_df.index, pd.DatetimeIndex)

    def test_metadata_copyrestore(self):
        """ Test function for copy_metadata() and restore_metadata() """
        my_df = smsc.to_dataframe(['COL1', 'My Sample Time'],
                                  ['7, 2000-01-01 00:00:01',
                                   '23, 2000-01-01 00:01:00',
                                   '30, 2000-01-01 00:01:58'],
                                  {'system': 'LOCAL',
                                   'missing': 107,
                                   'repeated': False})
        metadata_bck = smsc.copy_metadata(my_df)
        empty_df = pd.DataFrame()
        smsc.restore_metadata(metadata_bck, empty_df)
        # Check that empty_df._metadata values are copied
        for item in my_df._metadata:
            self.assertIn(item, empty_df._metadata)

    def test_plotvar(self):
        """ Test function for plot_var """
        with open(TEST_CSV, 'r') as filedescriptor:
            header, data_lines, metadata = smsc.extract_t4csv(filedescriptor)
        dataframe = smsc.to_dataframe(header, data_lines, metadata)
        object.__setattr__(dataframe, 'system', ('SYSTEM1',))
        myplot = smsc.plot_var(dataframe,
                               'FRONTEND_11_OUTPUT_OK',
                               logger=LOGGER)
        self.assertTrue(myplot.has_data())
        self.assertTrue(myplot.is_figure_set())

    def test_initlogger(self):
        """ Test function for init_logger """
        # Default options, loglevel is 20 (INFO)
        my_logger = smsc.init_logger()
        self.assertEqual(my_logger.level, 20)
        self.assertEqual(my_logger.name, smsc.__name__)
        my_logger = smsc.init_logger(loglevel='DEBUG', name='testset')
        self.assertEqual('DEBUG', logging.getLevelName(my_logger.level))
        self.assertEqual('testset', my_logger.name)

    def test_dataframize(self):
        """ Test function for dataframize """
        dataframe = smsc.dataframize(TEST_CSV, logger=LOGGER)
        self.assertTupleEqual(dataframe.shape, (286, 931))
        self.assertTrue(hasattr(dataframe, '_metadata'))
        # Check that metadata is in place and extra columns were created too
        for item in dataframe._metadata:
            self.assertTrue(hasattr(dataframe, item))
            self.assertIn(item, dataframe)

    def test_getstats(self):
        """ Test function for get_stats_from_host """
        df1 = smsc.get_stats_from_host('test',
                                             TEST_CSV,
                                             sftp_client='local',
                                             logger=LOGGER)
        df2 = smsc.read_pickle(TEST_PKL)
        assert_frame_equal(df1, df2)

    def test_inittunnels(self):
        """ Test function for init_tunnels """

    def test_getsyslogs(self):
        """ Test function for get_system_logs """

    def test_getsysdata(self):
        """ Test function for get_system_data """

    def test_serialmain(self):
        """ Test function for main (serial mode) """

    def test_threadedmain(self):
        """ Test function for main (threaded mode) """

if __name__ == '__main__':
    unittest.main()
