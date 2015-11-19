#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for df_tools.py
"""

from __future__ import absolute_import

import __builtin__
import csv
from re import split
from collections import OrderedDict
from cStringIO import StringIO
from itertools import takewhile

import numpy as np
import pandas as pd
from paramiko import SFTPClient

from .logger import init_logger
from t4mon import __version__


SEPARATOR = ','  # CSV separator, usually a comma
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"  # Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"  # End of Format-2 header
DATETIME_TAG = 'Sample Time'  # Column containing sample datetime
T4_DATE_FORMAT = '%Y-%b-%d %H:%M:%S.00'  # Format for date column

__all__ = ('select_var',  't4csv_to_plain', 'plain_to_t4csv',
           'to_dataframe', 'dataframize')


class ToDfError(Exception):

    """Exception raised while converting a CSV into a pandas dataframe"""
    pass


class ExtractCSVException(Exception):

    """Exception raised while extracting a CSV file"""
    pass


def consolidate_data(partial_dataframe, dataframe=None, system=None):
    """
    Consolidates partial_dataframe which corresponds to `system` with
    dataframe if provided, else pd.DataFrame()
    """
    if not isinstance(partial_dataframe, pd.DataFrame):
        raise ToDfError('Cannot consolidate with a non-dataframe object')
    if not isinstance(system, str) or not system:
        raise ToDfError('Need a system to consolidate the dataframe')
    if dataframe is None:
        dataframe = pd.DataFrame()
    # Add a secondary index based in the value of `system` in order to avoid
    # breaking cluster statistics, i.e. data coming from cluster LONDON and
    # represented by systems LONDON-1 and LONDON-2
    index_len = len(partial_dataframe.index)
    midx = pd.MultiIndex(
               levels=[partial_dataframe.index.get_level_values(0),
                       [system]],
               labels=[range(index_len), [0]*index_len],
               names=[partial_dataframe.index.names[0], 'system']
           )
    partial_dataframe = partial_dataframe.set_index(midx)
    return pd.concat([dataframe, partial_dataframe])


def reload_from_csv(csv_filename, plain=False):
    """
    Load a CSV into a dataframe
    Assumes that first column contains the timestamp information
    """
    if plain:  # plain CSV
        data = pd.read_csv(csv_filename, index_col=0)
    else:
        data = dataframize(csv_filename)
    data.index = pd.to_datetime(data.index)
    if plain:  # Restore the index name
        data.index.name = 'datetime'
    return data


def get_matching_columns(dataframe, var_names):
    """
    Filter column names that match first item in var_names, which can have
    wildcards ('*'), like 'str1*str2'; in that case the column name must
    contain both 'str1' and 'str2'.

    TODO: str1*str2 actually means str1*str2* now. It shouldn't.
    """
    if dataframe.empty:
        return []
    else:
        return [col for col in dataframe.columns
                for var_item in var_names
                if all([k in col.upper() for k in
                        var_item.upper().strip().split('*')])]


def find_in_iterable_case_insensitive(iterable, name):
    """
    From an iterable, return the value that matches `name`, case insensitive
    """
    iterupper = list(OrderedDict.fromkeys([k.upper() for k in iterable]))
    try:
        match = iterable[iterupper.index(name.upper())]
    except (ValueError, AttributeError):
        match = None

    return match


def select_var(dataframe, *var_names, **optional):
    """
    Returns selected variables that match columns from the dataframe.

    var_names: Filter column names that match any var_names; each individual
               var_item in var_names (1st one only if not filtering on system)
               can have wildcards ('*') like 'str1*str2'; in that case the
               column name must contain both 'str1' and 'str2'.
    optional: - split_by (filter or not based on that index level and content),
                only one filter allowed. Example: system='SYSTEM1'
              - logger (logging.Logger instance)
    """
    logger = optional.pop('logger', '') or init_logger()
    (ix_level, filter_by) = optional.popitem() if optional else (None,
                                                                 None)
    ix_levels = [level.upper() for level in dataframe.index.names if level]
    if ix_level and ix_level.upper() not in ix_levels:
        logger.warning('Bad filter found: "%s" not found in index '
                       '(case insensitive)',
                       ix_level)
        filter_by = None

    if ix_level:
        ix_level = find_in_iterable_case_insensitive(
                       iterable=dataframe.index.names,
                       name=ix_level
                   )
        try:
            if not filter_by:  # fallback if filter_by is not a valid value
                filter_by = dataframe.index.get_level_values(
                                ix_level
                            ).unique()[0]
            filter_by = find_in_iterable_case_insensitive(
                            iterable=dataframe.index.get_level_values(ix_level
                                                                      ),
                            name=filter_by
                        )
            _df = dataframe.xs(filter_by,
                               level=ix_level) if filter_by else pd.DataFrame()
        except KeyError:
            logger.warning('Value: "%s" not found in index level "%s"!',
                           filter_by,
                           ix_level)
            return pd.DataFrame()
    else:
        _df = dataframe

    # Drop all columns that have all values missing
    _df.dropna(axis=1, how='all')

    if len(var_names) == 0:
        logger.warning('No variables were selected, returning all '
                       'columns%s',
                       ' for level {}={}'.format(ix_level, filter_by)
                       if filter_by else '')
        return _df
    return _df[get_matching_columns(_df, var_names)]


def extract_t4csv(file_descriptor):
    """ Reads Format1/Format2 T4-CSV and returns:
         * field_names: List of strings (column names)
         * data_lines: List of strings (each one representing a sample)
         * metadata: Cluster name as found in the first line of Format1/2 CSV
    """
    try:
        data_lines = [li.rstrip()
                      for li in takewhile(lambda x:
                                          not x.startswith('Column Average'),
                                          file_descriptor)]
        _l0 = split(r'/|%c *| *' % SEPARATOR, data_lines[0])
        metadata = {'system': _l0[1] if _l0[0] == 'Merged' else _l0[0]}

        if START_HEADER_TAG in data_lines[1]:  # Format 2
            # Search from the bottom in case there's a format2 violation,
            # common with t4 merge where files are glued just as with cat,
            # so there are 2x headers, discarding the first part
            # Our header will be between [h_ini, h_last]
            h_ini = len(data_lines) - \
                data_lines[::-1].index(START_HEADER_TAG)
            h_last = len(data_lines) - \
                data_lines[::-1].index(END_HEADER_TAG)

            field_names = SEPARATOR.join(data_lines[h_ini:h_last-1]).\
                split(SEPARATOR)  # This is now a list with all the columns
            data_lines = data_lines[h_last:]
        else:  # Format 1
            field_names = data_lines[3].split(SEPARATOR)
            data_lines = data_lines[4:]
        return (field_names, data_lines, metadata)
    except:
        raise ExtractCSVException


def t4csv_to_plain(t4_csv, output):
    """ Convert a T4-compliant CSV file into plain (excel dialect) CSV file """
    data = reload_from_csv(t4_csv, plain=False)
    data.to_csv(output,
                date_format=T4_DATE_FORMAT)


def dataframe_to_t4csv(dataframe, output, t4format=2):
    """ Save dataframe to Format1/2 T4-compliant CSV file """
    # We must remove the 'system' column from the dataframe
    system_column = find_in_iterable_case_insensitive(dataframe.columns,
                                                      'system')
    data_sys = ','.join(np.unique(dataframe[system_column]))
    try:
        buffer_object = StringIO()
        dataframe.to_csv(buffer_object,
                         date_format=T4_DATE_FORMAT,
                         columns=dataframe.columns.drop(system_column))
        buffer_object.seek(0)
        _to_t4csv(buffer_object,
                  output=output,
                  t4format=t4format,
                  system_id=data_sys)
    finally:
        buffer_object.close()


def plain_to_t4csv(plain_csv, output, t4format=2):
    """ Convert plain CSV into T4-compliant Format1/2 CSV file """
    data = reload_from_csv(plain_csv)
    dataframe_to_t4csv(dataframe=data,
                       output=output,
                       t4format=t4format)


def _to_t4csv(file_object, output, t4format=2, system_id=None):
    """ Save file_object contents to Format1/2 T4-compliant CSV file"""
    if t4format not in [1, 2]:
        raise AttributeError('Bad T4-CSV format {} (must be either 1 '
                             'or 2)'.format(t4format))
    with open(output, 'w') as csvfile:
        csvfile.write('{}, t4Monitor Version {}\n'.format(
                      system_id or 'SYSTEM', __version__)
                      )
        if t4format == 2:
            csvfile.write('{}\n'.format(START_HEADER_TAG))
            csvfile.write(file_object.readline())  # Fields in 1st line
            csvfile.write('{}\n'.format(END_HEADER_TAG))
        csvfile.write(file_object.read())


def to_dataframe(field_names, data, metadata):
    """
    Loads CSV data into a pandas DataFrame
    Return an empty DataFrame if fields and data aren't correct,
    otherwhise it will interpret it with NaN values.
    Column named DATETIME_TAG (i.e. 'Sample Time') is used as index
    """
    _df = pd.DataFrame()  # default to be returned if exception is found
    try:
        if field_names and data:  # else return empty dataframe
            # put data in a file object and send it to pd.read_csv()
            fbuffer = StringIO()
            fbuffer.writelines(('%s\n' % line for line in data))
            fbuffer.seek(0)
            # Multiple columns may have a sample time, parse dates from all
            df_timecol = (s for s in field_names if DATETIME_TAG in s).next()
            if df_timecol == '':
                raise ToDfError
            _df = pd.read_csv(fbuffer, names=field_names,
                              parse_dates={'datetime': [df_timecol]},
                              index_col='datetime')
    except Exception as exc:
        raise ToDfError(exc)
    return _df


def dataframize(a_file, sftp_session=None, logger=None):
    """
    Wrapper for to_dataframe, leading with non-existing files over sftp
    If sftp_session is not a valid session, work with local filesystem
    """

    logger = logger or init_logger()
    logger.info('Loading file %s...', a_file)
    try:
        if not isinstance(sftp_session, SFTPClient):
            sftp_session = __builtin__  # open local file
        with sftp_session.open(a_file) as file_descriptor:
            _single_df = to_dataframe(*extract_t4csv(file_descriptor))
        return _single_df
    except IOError:  # non-existing files also return an empty dataframe
        logger.error('File not found: %s', a_file)
        return pd.DataFrame()
    except ExtractCSVException:
        logger.error('An error occured while extracting the CSV file: %s',
                     a_file)
        return pd.DataFrame()
    except ToDfError:
        logger.error('Error occurred while internally processing CSV file: %s',
                     a_file)
        return pd.DataFrame()
