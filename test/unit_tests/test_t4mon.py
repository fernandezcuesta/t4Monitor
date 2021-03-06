#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for __init__.py methods
"""
from __future__ import absolute_import

import tempfile
import unittest

import t4mon as init_func


class TestInit(unittest.TestCase):

    """ Set of unit tests for __init__.py """

    def test_dump_config(self):
        """ Test function for dump_config """
        with tempfile.SpooledTemporaryFile(mode='w') as temp_file:
            init_func.dump_config(temp_file)
            temp_file.seek(0)
            config_dump = temp_file.readlines()

        self.assertGreater(len(config_dump), 0)
        self.assertTrue(any(['DEFAULT' in line for line in config_dump]))
