#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - SMSC monitoring **test functions**
"""
from __future__ import absolute_import

import tempfile
import unittest

import t4mon as init_func


class TestInit(unittest.TestCase):
    """ Set of test functions for __init__.py """

    def test_dump_config(self):
        """ Test function for dump_config """
        with tempfile.SpooledTemporaryFile() as temp_file:
            init_func.dump_config(temp_file)
            temp_file.seek(0)
            config_dump = temp_file.readlines()

        self.assertGreater(len(config_dump), 0)
        self.assertTrue(any(['DEFAULT' in line for line in config_dump]))
