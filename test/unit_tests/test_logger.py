#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for logger.py
"""
import logging
import unittest
from logging import handlers

from t4mon import logger


class TestLogger(unittest.TestCase):

    """ Set of test functions for logger.py """

    def test_initlogger(self):
        """ Test function for init_logger """
        my_logger = logger.init_logger()
        self.assertEqual(my_logger.level, logging.DEBUG)
        self.assertEqual(my_logger.name, logger.__name__)
        # Even if the loglevel is set to a different value, the logger loglevel
        # stays in 'DEBUG'
        my_logger = logger.init_logger(loglevel=logging.INFO, name='testset')
        self.assertEqual(my_logger.level, logging.DEBUG)
        self.assertEqual('testset', my_logger.name)

    def test_init_logger_has_two_handlers(self):
        my_logger = logger.init_logger()
        self.assertEqual(len(my_logger.handlers), 2)
        # Check there's one StreamHandler and one FileHandler
        self.assertTrue(any([isinstance(_handler, logging.StreamHandler)
                             for _handler in my_logger.handlers]))
        self.assertTrue(any([isinstance(_handler,
                                        handlers.TimedRotatingFileHandler)
                             for _handler in my_logger.handlers]))

        # Check that file handler's loglevel is the same as logger loglevel
        # Check that console handler's loglevel is the same as specified
        for _handler in my_logger.handlers:
            print("%s <> %s" % (logger.DEFAULT_LOGLEVEL,
                                _handler.level))
            if isinstance(_handler, handlers.TimedRotatingFileHandler):
                self.assertEqual(_handler.level, my_logger.level)
            else:
                self.assertEqual(_handler.level,
                                 logger.DEFAULT_LOGLEVEL)
