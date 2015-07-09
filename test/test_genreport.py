#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*pysmscmon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import unittest
import numpy as np
import pandas as pd
import imghdr
import tempfile

from pysmscmon import df_tools
import pysmscmon as init_func
from pysmscmon.gen_report import gen_report, get_graphs
from pysmscmon import logger

TEST_DATAFRAME = pd.DataFrame(np.random.randn(100, 4),
                              columns=['test1',
                                       'test2',
                                       'test3',
                                       'test4'])
TEST_CSV = 'test/test_data.csv'
TEST_PKL = 'test/test_data.pkl'
TEST_GRAPHS_FILE = 'test/test_graphs.cfg'
TEST_HTMLTEMPLATE = 'test/test_template.html'

LOGGER = logger.init_logger(loglevel='DEBUG', name='test-pysmscmon')


class TestGenReport(unittest.TestCase):
    """ Test functions for gen_report.py """
    def test_genreport(self):
        """ Test function for gen_report """
        my_container = init_func.Container(loglevel='keep')
        # fill it with some data
        my_container.logger = LOGGER
        my_container.data = df_tools.read_pickle(TEST_PKL)
        my_container.system = my_container.data.system.upper()
        my_container.logs[my_container.system] = 'Skip logs here, just a test!'
        my_container.html_template = TEST_HTMLTEMPLATE
        my_container.graphs_file = TEST_GRAPHS_FILE

        html_rendered = gen_report(my_container)
        self.assertIn('<title>Monitoring of {} at {}</title>'.format(
            my_container.system,
            my_container.date_time),
                      html_rendered)

        graph_titles = []
        with open(my_container.graphs_file, 'r') as graphs_file:
            for line in graphs_file:
                line = line.strip()
                if not len(line) or line[0] == '#':
                    continue
                graph_titles.append(line.split(';')[1])

        for title in graph_titles:
            title = title.strip()
            self.assertIn('<pre><gtitle>{}</gtitle></pre>'.format(title),
                          html_rendered)

        # Test with a non existing template file, should return ''
        my_container.html_template = 'this_file_does_not_exist'
        self.assertEqual(gen_report(my_container), '')

        # Same with a bad formatted container
        my_container.system = None
        my_container.html_template = TEST_HTMLTEMPLATE
        self.assertEqual(gen_report(my_container), '')

    def test_getgraphs(self):
        """ Test function for get_graphs """
        my_container = init_func.Container(loglevel='keep')
        my_container.logger = LOGGER
        my_container.data = df_tools.read_pickle(TEST_PKL)
        my_container.system = my_container.data.system.upper()
        my_container.logs[my_container.system] = 'Skip logs here, just a test!'
        my_container.html_template = TEST_HTMLTEMPLATE
        my_container.graphs_file = TEST_GRAPHS_FILE

        my_graph = get_graphs(my_container).next()
        # test that the generated graph is a valid b64 encoded png
        self.assertIsInstance(my_graph, tuple)
        for tuple_element in my_graph:
            self.assertIsInstance(tuple_element, str)
        my_graph = my_graph[1].replace('data:image/png;base64,', '')
        with tempfile.NamedTemporaryFile() as temporary_file:
            temporary_file.write(my_graph.decode('base64'))
            temporary_file.file.close()
            self.assertEqual(imghdr.what(temporary_file.name), 'png')

        # Test when the graphs file contains invalid entries
        my_container.graphs_file = TEST_CSV  # bad file here
        self.assertIsNone(get_graphs(my_container).next())
