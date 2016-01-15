#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Make simple arithmetical calculations on pandas DataFrame columns.
Configuration: calc_file

calc_file format:

# This is just a comment ##########################
# Inline comments may start either with # or ;
C = (A + B) / B  ; A and B are valid columns
D = (C + 100.0) / (A + C)  # lines are processed in order

"""
from __future__ import absolute_import

import re
import sys

from numbers import Number

TTAG = '__calculations_tmp'  # temporal column names tag


def oper(self, oper1, funct, oper2):
    """
    Returns funct(oper1, oper2)
    """
    try:
        if funct is '+':
            if isinstance(oper1, Number) or isinstance(oper2, Number):
                return oper1 + oper2
            return oper1.add(oper2)
        elif funct is '-':
            if isinstance(oper1, Number) or isinstance(oper2, Number):
                return oper1 - oper2
            return oper1.sub(oper2)
        elif funct is '/':
            if isinstance(oper1, Number) or isinstance(oper2, Number):
                return oper1 / oper2
            return oper1.div(oper2)
        if isinstance(oper1, Number) or isinstance(oper2, Number):
            return oper1 * oper2
        else:
            return oper1.mul(oper2)
    except (TypeError, ValueError):
        self.logger.warning("Might be an error in the equation, returning NaN")
        return float('NaN')


def oper_wrapper(self, oper1, funct, oper2):
    """
    Returns the operation defined by f {+-*/} for oper1 and oper2 for df
    """
    try:
        oper1 = float(oper1)
    except (ValueError, TypeError):
        pass
    try:
        oper2 = float(oper2)
    except (ValueError, TypeError):
        pass

    try:
        if funct not in '+-*/':
            self.logger.warning("Might be an error in the equation, "
                                "returning NaN")
        elif isinstance(oper1, float) and isinstance(oper2, float):
            return self.oper(oper1, funct, oper2)
        elif isinstance(oper1, float):
            return self.oper(oper1, funct, self[oper2])
        elif isinstance(oper2, float):
            return self.oper(self[oper1], funct, oper2)
        else:
            return self.oper(self[oper1], funct, self[oper2])
    except KeyError as exc:
        self.logger.warning('{0} not found in dataset, returning NaN'
                            .format(repr(exc)))
    except Exception as exc:
        self.logger.error('Returning NaN. Unexpected error during '
                          'calculations: {0}'.format(repr(exc)))
    return float('NaN')


def recursive_lis(self, sign_pattern, parn_pattern, res, c_list):
    """
    Recursively solve equation passed as c_list and store in self[res]
    """
    # Get rid of trailing spaces in result name
    res = res.strip()
    # Breaking into a list of strings when done,
    # if it is a string then we've some work to do...
    if isinstance(c_list, str):
        # text inside the parenthesis
        par = re.findall(parn_pattern, c_list)
        if par:
            # First parenthesis will be TTAG1, second will be TTAG2, ...
            tmp_tag = '{0}{1}'.format(TTAG,
                                      sum([TTAG in col for col in self]) + 1)
            # Resolve equation inside parenthesis and come back
            self.recursive_lis(sign_pattern, parn_pattern, tmp_tag, par[0])
            # Update c_list by replacing the solved equation by 'TTAGN'
            c_list = c_list.strip().replace('({0})'.format(par[0]), tmp_tag)
            # Solve the rest of the equation (yet another parenthesis?)
            self.recursive_lis(sign_pattern, parn_pattern, res, c_list)
        else:
            # Shouldn't be any parenthesis here
            c_list = re.split(sign_pattern, re.sub(' ', '', c_list))

    # Break the recursive loop if we already have the result
    if res in self:
        return
    try:
        if len(c_list) is 3:
            self[res] = self.oper_wrapper(*c_list)
        elif len(c_list) is 1:
            if re.match('^[\w\d\.]+$', c_list[0]) is None:
                self[res] = float('NaN')
            else:
                self[res] = c_list[0]
        else:
            self[res] = self.oper_wrapper(
                self.recursive_lis(sign_pattern,
                                   parn_pattern,
                                   res,
                                   c_list[:-2]),
                c_list[-2],
                c_list[-1].strip())
        return res.strip()
    except Exception as exc:
        _, _, exc_tb = sys.exc_info()
        self.logger.error('Unexpected exception at calculations (line {0}): '
                          '{1}'.format(exc_tb.tb_lineno, repr(exc)))
        return


def clean_calcs(self, calc_file):
    """
    Delete columns added by apply_lis
    """
    try:
        self.logger.info('Dataframe shape before cleanup: {0}'
                         .format(self.shape))
        with open(calc_file, 'r') as calcfile:
            colnames = [line.split('=')[0].strip() for line in calcfile
                        if line[0] not in ';#!/%[ ' and len(line) > 3]
            # self.drop(colnames, axis=1, inplace=True)
            for col in colnames:
                try:
                    self.drop(col, axis=1, inplace=True)
                    self.logger.debug('Deleted column: {0}'.format(col))
                except ValueError:
                    if len(col) > 20:  # truncate the column name if too long
                        col = '{0}...'.format(col[:20])
                    self.logger.debug('Error while cleaning column {0}'
                                      .format(col))
                    continue
        self.logger.info('Dataframe shape after cleanup: {0}'
                         .format(self.shape))
    except IOError:
        self.logger.error("Could not process calculation file: {0}"
                          .format(calc_file))


def clean_comments(line, pattern=None):
    """
    Clean commented lines or inline comments from a valid line
    Arguments:
    - line: string to be cleaned
    - pattern [optional]: regex compiled pattern, faster if an already compiled
                          pattern is sent
    Returns: string
    """
    if not pattern:
        pattern = re.compile(r'^([^#]*)[#;](.*)$')
    _match = re.match(pattern, line)
    if _match:
        return _match.group(1).strip()
    else:
        return line.strip()


def apply_calcs(self, calc_file, system=None):
    """
    Read calculations file, make the calculations and get rid of temporary data
    """
    try:
        # Define regex patterns
        arithmetic_pattern = re.compile(r'([+\-*/])')  # allowed functions
        parenthesis_pattern = re.compile(r'.*\(+([\w .+\-*/]+)\)+.*')
        comments_pattern = re.compile(r'^([^#]*)[#;](.*)$')
        with open(calc_file, 'r') as calcfile:
            for line in calcfile:
                line = clean_comments(line, comments_pattern)
                if not line:
                    continue
                self.logger.debug('{0}Processing: {1}'
                                  .format('{0} | '.format(system) if
                                          system else '',
                                          line))
                self.recursive_lis(arithmetic_pattern,
                                   parenthesis_pattern,
                                   *line.split('='))
        # Delete temporary columns (starting with TTAG)
        for colname in self.columns[[TTAG in col for col in self]]:
            del self[colname]
    except IOError:
        self.logger.error("Could not process calculation file: {0}"
                          .format(calc_file))
