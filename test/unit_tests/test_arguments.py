#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for arguments.py
"""
from __future__ import absolute_import

import os
import unittest

from mock import patch
from t4mon import arguments
from six.moves import configparser
from t4mon.logger import DEFAULT_LOGLEVEL

from . import base


class TestArguments(unittest.TestCase):

    def test_get_absolute_path(self):
        """ Test auxiliary function get_absolute_path """
        self.assertEqual(
            arguments.get_absolute_path(),
            os.path.dirname(os.path.abspath(
                arguments.DEFAULT_SETTINGS_FILE
            )) + os.sep
        )
        self.assertEqual(
            arguments.get_absolute_path('file.txt', '/home/user/other.txt'),
            '/home/user/file.txt'
        )

    def test_config(self):
        """ test function for read_config """
        config = arguments.read_config(base.TEST_CONFIG)
        self.assertIsInstance(config, configparser.SafeConfigParser)
        self.assertGreater(len(config.sections()), 2)
        self.assertIn('GATEWAY', config.sections())
        self.assertTrue(all([key in [i[0] for i in config.items('DEFAULT')]
                             for key in ['ssh_port', 'ssh_timeout',
                                         'tunnel_port', 'folder', 'username',
                                         'ip_or_hostname']]))
        # Trying to read a bad formatted config file should raise an exception
        self.assertRaises(arguments.ConfigReadError,
                          arguments.read_config,
                          base.TEST_CSV)

    def test_parse_arguments_main(self):
        parser = arguments._parse_arguments_main(
            ['--all',
             '--settings={}'.format(base.BAD_CONFIG),
             '--loglevel=DEBUG']
        )
        self.assertTrue(parser['alldays'])
        self.assertEqual(parser['loglevel'], 'DEBUG')
        self.assertFalse(parser['nologs'])
        self.assertFalse(parser['noreports'])
        self.assertEqual(parser['settings_file'], base.BAD_CONFIG)
        self.assertFalse(parser['safe'])

    @patch('t4mon.arguments.__get_input', return_value='y')
    def test_answer_yes_when_no_arguments_entered(self, input):
        """
        Test that all defaults values are returned by parser when
        no arguments are passed and 'y' ys selected.
        """
        parser = arguments._parse_arguments_main([])
        self.assertFalse(parser['alldays'])
        self.assertEqual(parser['loglevel'], DEFAULT_LOGLEVEL)
        self.assertFalse(parser['nologs'])
        self.assertFalse(parser['noreports'])
        self.assertEqual(parser['settings_file'],
                         arguments.DEFAULT_SETTINGS_FILE)
        self.assertFalse(parser['safe'])

    @patch('t4mon.arguments.__get_input', return_value='n')
    def test_answer_no_when_no_arguments_entered(self, input):
        """
        Test that an sys.exit is raised if answered 'n'
        """
        with self.assertRaises(SystemExit):
            arguments._parse_arguments_main([])

    @patch('t4mon.arguments.__get_input')
    def test_wrong_answer_when_no_arguments_entered(self, mock):
        """
        Test nothing's broken when answer is not ['Y', 'y', 'N', 'n']
        """
        with self.assertRaises(SystemExit):
            mock.side_effect = ['k', 'l', 'yeah', 'N']
            arguments._parse_arguments_main([])

    @patch('t4mon.arguments.__get_input')
    def test_insufficient_arguments_raise_error(self, mock):
        """
        Test that a sys.exit is raised if no CSV or PKL input files
        are specified for _parse_arguments_local
        """
        with self.assertRaises(SystemExit):
            mock.side_effect = ['k', 'l', 'yeah', 'Y']  # go on with defaults
            arguments._parse_arguments_local([])
