#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for df_tools.py
"""

from __future__ import absolute_import

import __builtin__
import os.path
from cStringIO import StringIO
from itertools import takewhile
from collections import OrderedDict

import pandas as pd
import numpy as np
from paramiko import SFTPClient

import t4mon

from .logger import init_logger

SEPARATOR = ','  # CSV separator, usually a comma
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"  # Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"  # End of Format-2 header
DATETIME_TAG = 'Sample Time'  # Column containing sample datetime
T4_DATE_FORMAT = '%Y-%b-%d %H:%M:%S.00'  # Format for date column

__all__ = ('select_var',  't4csv_to_plain', 'plain_to_t4csv',
           'to_dataframe', 'dataframize')


class ToDfError(Exception):

    """
    Exception raised while converting a CSV into a pandas dataframe
    """
    pass


class ExtractCSVException(Exception):

    """
    Exception raised while extracting a CSV file
    """
    pass


def remove_duplicate_columns(dataframe):
    """
    Remove columns with duplicate names from a dataframe
    """
    columns = list(dataframe.columns)
    field_names = list(OrderedDict.fromkeys((f for f in columns)))
    unique_columns = [columns.index(field_names[k])
                      for k in range(len(field_names))]
    return dataframe[unique_columns]


def consolidate_data(partial_dataframe, dataframe=None, system=None):
    """
    Consolidate partial_dataframe which corresponds to `system` with
    dataframe if provided, else pd.DataFrame()
    """
    if not isinstance(partial_dataframe, pd.DataFrame):
        raise ToDfError('Cannot consolidate with a non-dataframe object')
    if not isinstance(system, str) or not system:
        raise ToDfError('Need a system to consolidate the dataframe')
    if dataframe is None:
        dataframe = pd.DataFrame()
    partial_dataframe = add_secondary_index(partial_dataframe, system)
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
        data.index.name = DATETIME_TAG
    return data


def get_matching_columns(dataframe, var_names):
    """
    Filter column names that match first item in var_names, which can have
    wildcards ('*'), like 'str1*str2'; in that case the column name must
    contain both 'str1' and 'str2'.
    """
    # TODO: str1*str2 actually means str1*str2* now. It shouldn't.
    if dataframe.empty:
        return []
    else:
        return [col for col in dataframe.columns
                for var_item in var_names
                if all([k in col.upper() for k in
                        var_item.upper().strip().split('*')])]


def find_in_iterable_case_insensitive(iterable, name):
    """
    Return the value that matches `name`, case insensitive from an iterable
    """
    iterable = list(OrderedDict.fromkeys([k for k in iterable]))
    iterupper = [k.upper() for k in iterable]
    try:
        match = iterable[iterupper.index(name.upper())]
    except (ValueError, AttributeError):
        match = None

    return match


def select_var(dataframe, *var_names, **optional):
    """
    Return selected variables that match columns from the dataframe.

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
        return pd.DataFrame()

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
    """
    Read Format1/Format2 T4-CSV and return:
        - field_names: List of strings (column names)
        - data_lines: List of strings (each one representing a sample)
    """
    try:
        data_lines = [li.rstrip()
                      for li in takewhile(lambda x:
                                          not x.startswith('Column Average'),
                                          file_descriptor)]
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
        return (field_names, data_lines)
    except:
        raise ExtractCSVException


def t4csv_to_plain(t4_csv, output):
    """
    Convert a T4-compliant CSV file into plain (excel dialect) CSV file
    """
    data = reload_from_csv(t4_csv, plain=False)
    data.to_csv(output,
                date_format=T4_DATE_FORMAT)


def dataframe_to_t4csv(dataframe, output, t4format=2):
    """
    Save dataframe to Format1/2 T4-compliant CSV files, one per system
    Return: dict with the matching {system: filename}
    """
    output_names = {}
    for system in dataframe.index.levels[1]:
        data = dataframe.xs(system, level='system')
        try:
            buffer_object = StringIO()
            data.to_csv(buffer_object,
                        date_format=T4_DATE_FORMAT)
            buffer_object.seek(0)
            (_dir, output) = os.path.split(output)
            output = '{0}{1}{3}_{2}{4}'.format(_dir,
                                               os.sep if _dir else '',
                                               system,
                                               *os.path.splitext(output))
            output_names[system] = output
            _to_t4csv(buffer_object,
                      output=output,
                      t4format=t4format,
                      system_id=system)
        finally:
            buffer_object.close()
    return output_names


def add_secondary_index(dataframe, system):
    """
    Get a 2-indices dataframe from a dataframe with a DateTimeIndex index
    with (dataframe.DateTimeIndex, system) as the new index
    """
    # Add a secondary index based in the value of `system` in order to avoid
    # breaking cluster statistics, i.e. data coming from cluster LONDON and
    # represented by systems LONDON-1 and LONDON-2
    index_len = len(dataframe.index)
    midx = pd.MultiIndex(
               levels=[dataframe.index.get_level_values(0),
                       [system]],
               labels=[range(index_len), [0]*index_len],
               names=[DATETIME_TAG, 'system']
           )
    return dataframe.set_index(midx)


def plain_to_t4csv(plain_csv, output, t4format=2, system=None):
    """
    Convert plain CSV into T4-compliant Format1/2 CSV file
    """
    data = reload_from_csv(plain_csv, plain=True)
    if not system:  # if no system, just set the file name as system
        system = os.path.splitext(os.path.basename(plain_csv))[0]
    data = add_secondary_index(data, system)
    dataframe_to_t4csv(dataframe=data,
                       output=output,
                       t4format=t4format)


def _to_t4csv(file_object, output, t4format=2, system_id=None):
    """
    Save file_object contents to Format1/2 T4-compliant CSV file
    """
    if t4format not in [1, 2]:
        raise AttributeError('Bad T4-CSV format {} (must be either 1 '
                             'or 2)'.format(t4format))
    with open(output, 'w') as csvfile:
        csvfile.write('{0}, t4Monitor Version: {1}, '
                      'File Type Format {2}\n'.format(
                          system_id or 'SYSTEM',
                          t4mon.__version__,
                          t4format
                      ))
        if t4format == 2:
            csvfile.write('{}\n'.format(START_HEADER_TAG))
            csvfile.write(file_object.readline())  # Fields in 1st line
            csvfile.write('{}\n'.format(END_HEADER_TAG))
        csvfile.write(file_object.read())


def to_dataframe(field_names, data):
    """
    Load CSV data into a pandas DataFrame
    Return an empty DataFrame if fields and data aren't correct,
    otherwhise it will interpret it with NaN values.

    Column named DATETIME_TAG (i.e. 'Sample Time') is used as index
    It is common in T4 files to have several columns with a sample time, most
    probably due to an horizontal merge of different CSVs. In those cases the
    first column having 'Sample Time' on its name will be used.
    """
    _df = pd.DataFrame()  # default to be returned if exception is found
    try:
        if field_names and data:  # else return empty dataframe
            # put data in a file object and send it to pd.read_csv()
            fbuffer = StringIO()
            fbuffer.writelines(('%s\n' % line for line in data))
            fbuffer.seek(0)
            # Multiple columns may have a 'sample time' alike column,
            # only use first (case insensitive search)
            df_timecol = (s for s in field_names
                          if DATETIME_TAG.upper() in s.upper())
            index_col = df_timecol.next()
            _df = pd.read_csv(fbuffer,
                              header=None,
                              parse_dates=index_col,
                              index_col=index_col,
                              names=field_names)
            # Remove redundant time columns (if any)
            _df.drop(df_timecol, axis=1, inplace=True)
            # Remove duplicate columns to avoid problems with combine_first()
            _df = remove_duplicate_columns(_df)

    except (StopIteration, Exception) as exc:  # Not t4-compliant!
        raise ToDfError(exc)

    return _df


def dataframize(data_file, sftp_session=None, logger=None):
    """
    Wrapper for to_dataframe, leading with non-existing files over sftp.
    If sftp_session is not a valid session, work with local filesystem
    """

    logger = logger or init_logger()
    logger.info('Loading file %s...', data_file)
    try:
        if not isinstance(sftp_session, SFTPClient):
            sftp_session = __builtin__  # open local file
        with sftp_session.open(data_file) as file_descriptor:
            _single_df = to_dataframe(*extract_t4csv(file_descriptor))
        return _single_df
    except IOError:  # non-existing files also return an empty dataframe
        logger.error('File not found: %s', data_file)
        return pd.DataFrame()
    except ExtractCSVException:
        logger.error('An error occured while extracting the CSV file: %s',
                     data_file)
        return pd.DataFrame()
    except ToDfError:
        logger.error('Error occurred while processing CSV file: %s',
                     data_file)
        return pd.DataFrame()


def remove_outliers(dataframe, n_std=2):
    """
    Remove all rows that have outliers in at least one column from a dataframe
    by default larger than n_std standard deviations from the mean, in absolute
    value, by default 2-std.
    """
    return dataframe[dataframe.apply(lambda x:
                                     np.abs(x - x.mean()) <= n_std * x.std()
                                     ).all(axis=1)]
