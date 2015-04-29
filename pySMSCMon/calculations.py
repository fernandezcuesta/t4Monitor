#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Make the calculations on pd.DataFrame columns as defined in calc_file
"""

import pandas as pd
import re


TTAG = '_tmp'


def oper(self, oper1, funct, oper2, logger=None):
    """ Returns funct(oper1, oper2) """
    try:
        if funct == '+':
            return oper1 + oper2
        elif funct == '-':
            return oper1 - oper2
        elif funct == '/':
            return oper1 / oper2
        else:
            return oper1 * oper2
    except TypeError or ValueError:
        logger.warning("Might be an error in the equation, returning NaN")
        return float('NaN')
        
      
def oper_wrapper(self, oper1, funct, oper2, logger=None):
    """
    Returns the operation defined by f {+-*/} for oper1 and oper2 for df
    """
    try:
        oper1 = float(oper1)
    except ValueError:
        pass
    try:
        oper2 = float(oper2)
    except ValueError:
        pass
       
    try:   
        if funct not in '+-*/':
            logger.warning("Might be an error in the equation, returning NaN")
            return float('NaN')
        elif isinstance(oper1, float):
            return self.oper(oper1, funct, self[oper2], logger)
        elif isinstance(oper2, float):
            return self.oper(self[oper1], funct, oper2, logger)
        else:
            return self.oper(self[oper1], funct, self[oper2], logger)
    except Exception as exc:
        logger.error('Returning NaN. Unexpected error during calculations: %s',
                     repr(exc))
        return float('NaN')
      


def recursive_lis(self, sign_pattern, parn_pattern, logger, res, c_list):
    """
    Recursively solve equation passed as c_list and store in self[res]
    """

    # Get rid of trailing spaces in result name
    res = res.strip()
#    print "Equation: %s = %s" % (res, c_list)
    # Breaking into a list of strings when done, if string we've some work to do
    if isinstance(c_list, str):
        # text inside the parenthesis
        par = re.findall(parn_pattern, c_list)
        if par:
            # First parenthesis will be TTAG1, second will be TTAG2, ...
            tmp_tag = '%s%i' % (TTAG, sum([TTAG in col for col in self]) + 1)
            # Resolve equation inside parenthesis and come back
            self.recursive_lis(sign_pattern, parn_pattern, logger,
                               tmp_tag, par[0])
            # Update c_list by replacing the solved equation by 'TTAGN'
            c_list = c_list.strip().replace('(%s)' %par[0], tmp_tag)
#            print "PAR = ", par
#            print "clist = ", c_list
            # Solve the rest of the equation (yet another parenthesis?)
            self.recursive_lis(sign_pattern, parn_pattern, logger, res, c_list)
        else:
#            print "Shouldn't be any parenthesis here:", c_list
            c_list = re.split(sign_pattern, re.sub(' ', '', c_list))
    # Break the recursive loop if we already have the result
    if res in self:
        return
    try:
        if len(c_list) == 3:
            self[res] = self.oper_wrapper(*c_list, logger=logger)
        elif len(c_list) == 1:
            return c_list
        else:
            self[res.strip()] = self.oper_wrapper(\
                                    self.recursive_lis(sign_pattern,
                                                       parn_pattern,
                                                       logger,
                                                       res,
                                                       c_list[:-2]
                                                      ),
                                    c_list[-2],
                                    c_list[-1].strip().
                                    logger)
        return res.strip()
    except:
        logger.error("Error in equation")
        return  


def apply_lis(self, calc_file, logger):
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
                    logger.debug('%s | Processing line (length:%s): %s',
                                 list(self.system)[0],
                                 len(line),
                                 line.strip())
                    self.recursive_lis(sign_pattern,
                                       parn_pattern,
                                       logger,
                                       *line.strip().split('='))
        # Delete temporary columns (starting with TTAG)   
        for colname in self.columns[[TTAG in col for col in self]]:
            del self[colname]
    except IOError:
        logger.error("Error applying calculations, dataframe was not modified")


if __name__ == "__main__":
    pd.DataFrame.oper = oper
    pd.DataFrame.oper_wrapper = oper_wrapper
    pd.DataFrame.recursive_lis = recursive_lis
    pd.DataFrame.apply_lis = apply_lis


