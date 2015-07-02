#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 24 19:12:58 2015

@author: fernandezjm
"""
from __future__ import absolute_import
import logging

DEFAULT_LOGLEVEL = 'INFO'


def init_logger(loglevel=None, name=__name__):
    """ Initialize logger, sets the appropriate level and attaches a console
        handler.
    """
    logger = logging.getLogger(name)
    logger.setLevel(loglevel or DEFAULT_LOGLEVEL)

    # If no console handlers yet, add a new one
    if not any(isinstance(x, logging.StreamHandler) for x in logger.handlers):
        console_handler = logging.StreamHandler()
        if logging.getLevelName(logger.level) == 'DEBUG':
            _fmt = '%(asctime)s| %(levelname)-4.3s|%(threadName)10.9s/' \
                   '%(lineno)04d@%(module)-10.9s| %(message)s'
            console_handler.setFormatter(logging.Formatter(_fmt))
        else:
            console_handler.setFormatter(
                logging.Formatter('%(asctime)s| %(levelname)-8s| %(message)s'))
        logger.addHandler(console_handler)

    logger.info('Initialized logger with level: %s',
                logging.getLevelName(logger.level))
    return logger
