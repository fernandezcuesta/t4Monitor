#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for calculations.py
"""
from __future__ import absolute_import

import re
import math
import tempfile
import unittest

import pandas as pd
from pandas.util.testing import assert_frame_equal

from t4mon import collector

from .base import LOGGER


class TestCalculations(unittest.TestCase):

    """ Test functions for calculations.py """

    def __init__(self, *args):
        super(self.__class__, self).__init__(*args)
        self.testdf = pd.DataFrame({'A': 2,
                                    'B': 8,
                                    'C': [16] * 4})

    @classmethod
    def setUpClass(cls):
        collector.add_methods_to_pandas_dataframe(LOGGER)

    def test_oper(self):
        """ Test function for oper """
        # scalar .oper. scalar
        self.assertEqual(self.testdf.oper(2, '+', 3), 5)
        # dataframe .oper. scalar
        self.assertTrue(all([self.testdf.oper(self.testdf.A, '*', 4)[k] ==
                             self.testdf.B[k] for k in self.testdf.index]))
        # dataframe .oper. dataframe
        self.assertTrue(all([self.testdf.oper(self.testdf.C,
                                              '/',
                                              self.testdf.B)[k] ==
                             self.testdf.A[k] for k in self.testdf.index]))
        # Returns NaN when TypeError
        self.assertTrue(math.isnan(self.testdf.oper(2, '+', 'c')))

    def test_oper_wrapper(self):
        """ Test function for oper_wrapper """
        self.assertTrue(all([self.testdf.oper_wrapper('A', '*', '4')[k] ==
                             self.testdf.B[k]
                             for k in self.testdf.index]))
        self.assertTrue(all([self.testdf.oper_wrapper('A', '*', '4')[k] ==
                             self.testdf.B[k]
                             for k in self.testdf.index]))
        # now with a wrong column, it should return a single NaN
        self.assertTrue(math.isnan(self.testdf.oper_wrapper('A', '-', 'BAD')))
        # same while operator not supported
        self.assertTrue(math.isnan(self.testdf.oper_wrapper('A', '|', 'B')))

    def test_recursive_lis(self):
        """ Test function for recursive_lis """
        sign_pattern = re.compile(r'([+\-*/])')  # functions
        parn_pattern = re.compile(r'.*\(+([\w .+\-*/]+)\)+.*')  # parenthesis
        self.testdf.recursive_lis(sign_pattern,
                                  parn_pattern,
                                  'R',
                                  'B * C / (64 - 32)')
        self.assertTrue(all([self.testdf.R[k] == 4
                             for k in self.testdf.index]))

    def test_apply_calcs(self):
        """ Test function for apply_calcs """
        with tempfile.NamedTemporaryFile() as calcs_file:
            calcs_file.write('D = B * A\n')  # D = 8 * 2 = 16
            calcs_file.write('E = B * 2 - A + C\n')  # E = 8*2 - 2 + 16 = 30
            calcs_file.write('F = B * (2 - A) + C\n')  # F = 8*(2-2) + 16 = 16
            calcs_file.write('G = B ^ 3.0\n')  # Not supported, return NaN
            calcs_file.file.close()
            self.testdf.apply_calcs(calcs_file.name)

        self.assertTrue(all([self.testdf.D[k] == 16
                             for k in self.testdf.index]))
        self.assertTrue(all([self.testdf.E[k] == 30
                             for k in self.testdf.index]))
        self.assertTrue(all([self.testdf.F[k] == 16
                             for k in self.testdf.index]))
        self.assertTrue(all([math.isnan(self.testdf.G[k])
                             for k in self.testdf.index]))

    def test_clean_calculations(self):
        """ Test function for clean_calculations """
        df_with_calcs = self.testdf.copy()
        with tempfile.NamedTemporaryFile() as calcs_file:
            calcs_file.write('D = B * A\n')
            calcs_file.write('E = B / 2 - A + C\n')
            calcs_file.write('F = B / 0\n')  # Division by zero
            calcs_file.file.close()

            df_with_calcs.apply_calcs(calcs_file.name)
            df_with_calcs.clean_calcs(calcs_file.name)

        assert_frame_equal(self.testdf, df_with_calcs)
        # Attempting to clean from a non-exising file does nothing
        self.assertIsNone(self.testdf.clean_calcs('non-existing-file'))
        self.testdf.clean_calcs('test/test_data.csv')  # wrong calcs file
