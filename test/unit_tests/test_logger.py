#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for logger.py
"""
import logging
import unittest

from t4mon import logger


class TestLogger(unittest.TestCase):
    """ Set of test functions for logger.py """
    def test_initlogger(self):
        """ Test function for init_logger """
        # Default options, loglevel is 20 (INFO)
        my_logger = logger.init_logger()
        self.assertEqual(my_logger.level, 20)
        self.assertEqual(my_logger.name, logger.__name__)
        my_logger = logger.init_logger(loglevel='DEBUG', name='testset')
        self.assertEqual('DEBUG', logging.getLevelName(my_logger.level))
        self.assertEqual('testset', my_logger.name)
