#!/usr/bin/env python2
# -*- coding: utf-8 -*-
""" Logging methods common to other submodules.
"""
import logging
from logging import handlers

DEFAULT_LOGLEVEL = logging.WARNING  #: Default console handler level (WARNING)
FILE_HANDLER = True  # Add a timed rotating file handler if no handlers yet


def init_logger(loglevel=None, name=__name__):
    """
    Initialize logger, sets the appropriate level and attaches:
     - File handler: always in ``DEBUG`` mode
     - Console handler: level configured as per loglevel

    Keyword Arguments:
        loglevel (Optional['CRITICAL'|'ERROR'|'WARNING'|'INFO'|'DEBUG']):
            logging level for the console handler
            (default :const:`DEFAULT_LOGLEVEL`)
        name (Optional[str]): logger name
    Return:
        logging.Logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # If no console handlers yet, add a new one
    if not any(isinstance(x, logging.Handler) for x in logger.handlers):
        if FILE_HANDLER:
            # Add a file handler with 'DEBUG' level
            log_name = '{0}.log'.format(__name__.split('.')[0])
            _handler = handlers.TimedRotatingFileHandler(filename=log_name,
                                                         when='H')
            _add_handler(logger,
                         handler=_handler,
                         loglevel=logging.DEBUG)

        # Add a console handler
        _handler = logging.StreamHandler()
        _add_handler(logger,
                     handler=_handler,
                     loglevel=loglevel)
        logger.info('Initialized logger with level: {0}'
                    .format(loglevel))
    return logger


def _add_handler(logger, handler=None, loglevel=None):
    """
    Add a handler to an existing logging.Logger object
    """
    handler.setLevel(loglevel or DEFAULT_LOGLEVEL)
    if handler.level == logging.DEBUG:
        _fmt = '%(asctime)s| %(levelname)-4.3s|%(threadName)10.9s/' \
               '%(lineno)04d@%(module)-10.9s| %(message)s'
        handler.setFormatter(logging.Formatter(_fmt))
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s| %(levelname)-8s| %(message)s'
        ))
    logger.addHandler(handler)
