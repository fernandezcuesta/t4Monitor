#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for gen_report.py
"""
from __future__ import absolute_import

import base64
import imghdr
import tempfile

import six
import pandas as pd
from t4mon.gen_report import Report, gen_report

from . import base


class TestGenReport(base.BaseTestClass):

    """ Test functions for gen_report.py """

    def setUp(self):
        super(TestGenReport, self).setUp()
        self.my_container = self.orchestrator_test.clone()
        # fill it with some data
        self.my_container.data = self.test_data
        self.system = self.test_data.index.levels[1].unique()[0].upper()

    def test_genreport(self):
        """ Test function for gen_report """

        html_rendered = ''.join((chunk for chunk in
                                 gen_report(self.my_container, self.system)))
        self.assertIn('<title>Monitoring of {} at {}</title>'
                      .format(self.system, self.my_container.date_time),
                      html_rendered)

        graph_titles = []
        with open(self.my_container.graphs_definition_file,
                  'r') as graphs_file:
            for line in graphs_file:
                line = line.strip()
                if not len(line) or line[0] == '#':
                    continue
                graph_titles.append(line.split(';')[1])

        for title in graph_titles:
            title = title.strip()
            self.assertIn('<pre><gtitle>{}</gtitle></pre>'.format(title),
                          html_rendered)

    def test_non_existing_template(self):
        """Test with a non existing template file, should yield nothing"""
        with self.assertRaises(StopIteration):
            _report = Report(self.my_container, self.system)
            _report.html_template = 'this_file_does_not_exist'
            _report.render()

    def test_not_valid_data(self):
        """
        Test than when no data in the container nothing should be yielded
        """
        with self.assertRaises(StopIteration):
            _report = Report(self.my_container, self.system)
            _report.data = pd.DataFrame()
            _report.render()

    def test_no_system_specified(self):
        """
        Test than when no system was specified, nothing should be yielded
        """
        with self.assertRaises(StopIteration):
            gen_report(self.my_container, '')

    def test_rendergraphs(self):
        """ Test function for render_graphs """
        _report = Report(self.my_container, self.system)
        my_graph = next(_report.render_graphs())
        # test that the generated graph is a valid base64 encoded PNG file
        self.assertIsInstance(my_graph, tuple)
        for tuple_element in my_graph:
            self.assertIsInstance(tuple_element, six.string_types)
        my_graph = my_graph[1][len('data:image/png;base64,'):]
        with tempfile.NamedTemporaryFile() as temporary_file:
            temporary_file.write(base64.b64decode(six.b(my_graph)))
            temporary_file.file.close()
            self.assertEqual(imghdr.what(temporary_file.name), 'png')

        # Test when the graphs file contains invalid entries
        _report.graphs_definition_file = base.TEST_CSV  # bad file here
        self.assertIsNone(next(_report.render_graphs()))
