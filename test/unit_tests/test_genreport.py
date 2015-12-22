#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for gen_report.py
"""
from __future__ import absolute_import

import imghdr
import tempfile

import pandas as pd

from t4mon.gen_report import Report, gen_report

from .base import (
    TEST_CSV,
    TEST_HTMLTEMPLATE,
    BaseTestClass
)


class TestGenReport(BaseTestClass):

    """ Test functions for gen_report.py """

    def test_genreport(self):
        """ Test function for gen_report """
        my_container = self.orchestrator_test.clone()
        # fill it with some data
        my_container.data = self.test_data
        system = self.test_data.index.levels[1].unique()[0].upper()

        html_rendered = gen_report(my_container, system)
        self.assertIn('<title>Monitoring of {} at {}</title>'.format(
                          system,
                          my_container.date_time),
                      html_rendered)

        graph_titles = []
        with open(my_container.graphs_definition_file, 'r') as graphs_file:
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
        self.assertEqual(gen_report(my_container, system), '')

        # Same when no data in the container or when no system was specified
        my_container.html_template = TEST_HTMLTEMPLATE
        my_container.data = pd.DataFrame()
        self.assertEqual(gen_report(my_container, system), '')
        self.assertEqual(gen_report(my_container, ''), '')

    def test_getgraphs(self):
        """ Test function for get_graphs """
        my_container = self.orchestrator_test.clone()
        my_container.data = self.test_data
        system = self.test_data.index.levels[1].unique()[0].upper()

        _report = Report(my_container, system)
        my_graph = _report.render_graphs().next()
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
        _report.graphs_definition_file = TEST_CSV  # bad file here
        self.assertIsNone(_report.render_graphs().next())
