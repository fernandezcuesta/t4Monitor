#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Make the calculations on pd.DataFrame columns as defined in calc_file
"""
from __future__ import absolute_import

import re
import sys

TTAG = '__calculations_tmp'


def oper(self, oper1, funct, oper2):
    """ Returns funct(oper1, oper2) """
    try:
        if funct is '+':
            return oper1 + oper2
        elif funct is '-':
            return oper1 - oper2
        elif funct is '/':
            return oper1 / oper2
        else:
            return oper1 * oper2
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
        self.logger.warning('%s not found in dataset, returning NaN',
                            repr(exc))
    except Exception as exc:
        self.logger.error('Returning NaN. Unexpected error during '
                          'calculations: %s', repr(exc))
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
            tmp_tag = '%s%i' % (TTAG, sum([TTAG in col for col in self]) + 1)
            # Resolve equation inside parenthesis and come back
            self.recursive_lis(sign_pattern, parn_pattern, tmp_tag, par[0])
            # Update c_list by replacing the solved equation by 'TTAGN'
            c_list = c_list.strip().replace('(%s)' % par[0], tmp_tag)
            # Solve the rest of the equation (yet another parenthesis?)
            self.recursive_lis(sign_pattern, parn_pattern, res, c_list)
        else:
            # print "Shouldn't be any parenthesis here:", c_list
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
        self.logger.error('Unexpected exception at calculations (line %s): %s',
                          exc_tb.tb_lineno,
                          repr(exc))
        return


def clean_calcs(self, calc_file):
    """ Delete columns added by apply_lis """
    try:
        self.logger.info('Dataframe shape before cleanup: %s', self.shape)
        with open(calc_file, 'r') as calcfile:
            colnames = [line.split('=')[0].strip() for line in calcfile
                        if line[0] not in ';#!/%[ ' and len(line) > 3]
            # self.drop(colnames, axis=1, inplace=True)
            for col in colnames:
                try:
                    self.drop(col, axis=1, inplace=True)
                    self.logger.debug('Deleted column: %s', col)
                except ValueError:
                    if len(col) > 20:  # truncate the column name if too long
                        col = '{}...'.format(col[:20])
                    self.logger.debug('Error while cleaning column %s', col)
                    continue
        self.logger.info('Dataframe shape after cleanup: %s', self.shape)
    except IOError:
        self.logger.error("Could not process calculation file: %s", calc_file)


def apply_calcs(self, calc_file, system=None):
    """
    Read calculations file, make the calculations and get rid of temporary data
    """
    try:
        # Define regex patterns for functions and parenthesis lookup
        sign_pattern = re.compile(r'([+\-*/])')  # functions
        parn_pattern = re.compile(r'.*\(+([\w .+\-*/]+)\)+.*')  # parenthesis
        with open(calc_file, 'r') as calcfile:
            for line in calcfile:
                if line[0] not in ';#!/%[ ' and len(line) > 3:
                    self.logger.debug('%sProcessing: %s',
                                      '%s | ' % system if system else '',
                                      line.strip())
                    self.recursive_lis(sign_pattern,
                                       parn_pattern,
                                       *line.strip().split('='))
        # Delete temporary columns (starting with TTAG)
        for colname in self.columns[[TTAG in col for col in self]]:
            del self[colname]
    except IOError:
        self.logger.error("Could not process calculation file: %s", calc_file)
