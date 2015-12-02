#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 24 19:12:58 2015

@author: fernandezjm
"""
import logging
from logging import handlers

DEFAULT_LOGLEVEL = 'ERROR'  # Only for console output


def init_logger(loglevel=None, name=__name__):
    """ Initialize logger, sets the appropriate level and attaches a console
        handler.
    """
    logger = logging.getLogger(name)
    logger.setLevel('DEBUG')

    # If no console handlers yet, add a new one
    if not any(isinstance(x, logging.Handler) for x in logger.handlers):
        # Add a file handler with 'DEBUG' level
        log_name = '{}.log'.format(__name__.split('.')[0])
        _handler = handlers.TimedRotatingFileHandler(filename=log_name,
                                                     when='H')
        _add_handler(logger,
                     handler=_handler,
                     loglevel='DEBUG')

        # Add a console handler
        _handler = logging.StreamHandler()
        _add_handler(logger,
                     handler=_handler)


    logger.info('Initialized logger with level: %s',
                logging.getLevelName(logger.level))
    return logger


def _add_handler(logger, handler=None, loglevel=DEFAULT_LOGLEVEL):
    """ Add a handler to an existing logging.Logger object """

    handler.setLevel(loglevel)
    if handler.level == 10:  # 'DEBUG'
        _fmt = '%(asctime)s| %(levelname)-4.3s|%(threadName)10.9s/' \
               '%(lineno)04d@%(module)-10.9s| %(message)s'
        handler.setFormatter(logging.Formatter(_fmt))
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s| %(levelname)-8s| %(message)s'
        ))
    logger.addHandler(handler)
