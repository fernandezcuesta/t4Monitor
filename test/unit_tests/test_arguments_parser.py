#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import unittest
from mock import patch

from t4mon import arguments_parser
from t4mon.logger import DEFAULT_LOGLEVEL
from t4mon.collector import DEFAULT_SETTINGS_FILE

from .base import BAD_CONFIG


class TestAuxiliaryFunctions(unittest.TestCase):

    def test_parse_arguments(self):
        parser = arguments_parser.parse_arguments(
            ['--all',
             '--settings={}'.format(BAD_CONFIG),
             '--loglevel=DEBUG']
        )
        self.assertTrue(parser['alldays'])
        self.assertEqual(parser['loglevel'], 'DEBUG')
        self.assertFalse(parser['nologs'])
        self.assertFalse(parser['noreports'])
        self.assertEqual(parser['settings_file'], BAD_CONFIG)
        self.assertFalse(parser['threaded'])

    @patch('t4mon.arguments_parser.get_input', return_value='y')
    def test_answer_yes_when_no_arguments_entered(self, input):
        """ Test that all defaults values are returned by parser when
        no arguments are passed and 'y' ys selected.
        """
        parser = arguments_parser.parse_arguments([])
        self.assertFalse(parser['alldays'])
        self.assertEqual(parser['loglevel'], DEFAULT_LOGLEVEL)
        self.assertFalse(parser['nologs'])
        self.assertFalse(parser['noreports'])
        self.assertEqual(parser['settings_file'], DEFAULT_SETTINGS_FILE)
        self.assertFalse(parser['threaded'])

    @patch('t4mon.arguments_parser.get_input', return_value='n')
    def test_answer_no_when_no_arguments_entered(self, input):
        """ Test that an sys.exit is raised if answered 'n'
        """
        with self.assertRaises(SystemExit):
            arguments_parser.parse_arguments([])

    @patch('t4mon.arguments_parser.get_input')
    def test_wrong_answer_when_no_arguments_entered(self, mock):
        """ Test nothing's broken when answer is not ['Y', 'y', 'N', 'n']
        """
        with self.assertRaises(SystemExit):
            mock.side_effect = ['k', 'l', 'yeah', 'N']
            arguments_parser.parse_arguments([])
