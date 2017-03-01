#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Dataframe-related functions used by other submodules.
"""

import re
import os.path
from itertools import takewhile
from collections import OrderedDict

from six import string_types, advance_iterator

import numpy as np
import t4mon
import pandas as pd
from paramiko import SFTPClient
from six.moves import builtins, cStringIO
from t4mon.logger import init_logger

SEPARATOR = ','  #: CSV separator
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"  #: Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"  #: End of Format-2 header
DATETIME_TAG = 'Sample Time'  #: Column containing sample datetime
T4_DATE_FORMAT = '%Y-%b-%d %H:%M:%S.00'  #: Format for date column


class ToDfError(Exception):

    """
    Exception raised while converting a CSV into a pandas dataframe
    """
    pass


class ExtractCSVException(Exception):

    """
    Exception raised while extracting a CSV
    """
    pass


def remove_duplicate_columns(dataframe):
    """
    Remove columns with duplicate names from a dataframe.

    Arguments:
        dataframe (pandas.DataFrame): Original DataFrame
    Return:
        ``pandas.DataFrame``
    """
    columns = list(dataframe.columns)
    field_names = list(OrderedDict.fromkeys((f for f in columns)))
    unique_columns = [columns.index(field_names[k])
                      for k in range(len(field_names))]
    return dataframe[unique_columns]


def consolidate_data(partial_dataframe, dataframe=None, system=None):
    """
    Consolidate partial_dataframe which corresponds to `system` with
    dataframe if provided, else ``pd.DataFrame()``.

    Arguments:
        partial_dataframe (pandas.DataFrame): Input single-index dataframe
    Keyword Arguments:
        dataframe (pandas.DataFrame): Optional dataframe to consolidate with
        system (str): system to which the data in partial_dataframe belongs
    Return:
        ``pandas.DataFrame``

    """
    if not isinstance(partial_dataframe, pd.DataFrame):
        raise ToDfError('Cannot consolidate with a non-dataframe object')
    if not isinstance(system, string_types) or not system:
        raise ToDfError('Need a system to consolidate the dataframe')
    if dataframe is None:
        dataframe = pd.DataFrame()
    partial_dataframe = _add_secondary_index(partial_dataframe, system)
    return pd.concat([dataframe, partial_dataframe])


def reload_from_csv(csv_filename, plain=False, index_col=0):
    """
    Load a CSV into a dataframe.
    Assumes that first column contains the time-stamp information.

    Arguments:
        csv_filename(str): Input CSV file name
    Keyword Arguments:
        plain(boolean): Whether the file is a plain (excel) CSV or T4-flavored
        index_col(int): Column number containing the time-stamp information
    Return:
        ``pandas.DataFrame``
    """
    if plain:  # plain CSV
        data = pd.read_csv(csv_filename, index_col=index_col)
    else:
        data = dataframize(csv_filename)
    data.index = pd.to_datetime(data.index)
    if plain:  # Restore the index name
        data.index.name = DATETIME_TAG
    return data


def get_matching_columns(dataframe, *args, **kwargs):
    """
    Filter column whose names match the regular expressions defined in
    ``args``.

    Args:
        dataframe (pandas.DataFrame): Input DataFrame
        \*args (List[str]): List of regular expressions matching column names
    Keyword Args:
        excluded (List[str]): Exclusion list (case insensitive)
    """
    if dataframe.empty:
        return []
    excluded = kwargs.get('excluded', None) or []
    if not isinstance(excluded, list):
        excluded = [excluded]
    regex = re.compile('^.*({0}).*$'.format('|'.join(args)),
                       re.IGNORECASE)
    return [match.group(0) for column in dataframe.columns
            for match in [re.search(regex, column)]
            if match and not any(exclusion.upper() in column.upper()
                                 for exclusion in excluded)
            ]


def _find_in_iterable_case_insensitive(iterable, name):
    """
    Return the value matching ``name``, case insensitive, from an iterable.
    """
    iterable = list(OrderedDict.fromkeys([k for k in iterable]))
    iterupper = [k.upper() for k in iterable]
    try:
        match = iterable[iterupper.index(name.upper())]
    except (ValueError, AttributeError):
        match = None

    return match


def select(dataframe, *args, **kwargs):
    """
    Get view of selected variables that match columns from the dataframe.

    Arguments:
        dataframe(pandas.DataFrame): Input data
        \*args(List[str]): List of regular expressions selecting column names
    Keyword Arguments:
        filter(str):
            Filter based on the index level and content, only one filter
            allowed. Example: ``system='SYSTEM1'``
        excluded(List[str]):
            Exclusion list, items matching this list (case insensitive) will
            not be selected.
        logger(logging.Logger): Optional logger instance

    Returns:
        ``pandas.DataFrame``


    """
    logger = kwargs.pop('logger', '') or init_logger()
    excluded = kwargs.pop('excluded', None)
    (ix_level, filter_by) = kwargs.popitem() if kwargs else (None, None)
    ix_levels = [level.upper() for level in dataframe.index.names if level]
    if ix_level and ix_level.upper() not in ix_levels:
        logger.warning('Bad filter found: "{0}" not found in index '
                       '(case insensitive)'.format(ix_level))
        return pd.DataFrame()

    if ix_level:
        ix_level = _find_in_iterable_case_insensitive(
            iterable=dataframe.index.names,
            name=ix_level
        )
        try:
            if not filter_by:  # fallback if filter_by is not a valid value
                filter_by = dataframe.index.get_level_values(
                    ix_level
                ).unique()[0]
            filter_by = _find_in_iterable_case_insensitive(
                iterable=dataframe.index.get_level_values(ix_level),
                name=filter_by
            )
            _df = dataframe.xs(filter_by,
                               level=ix_level) if filter_by else pd.DataFrame()
        except KeyError:
            logger.warning('Value: "{0}" not found in index level "{1}"!'
                           .format(filter_by, ix_level))
            return pd.DataFrame()
    else:
        _df = dataframe

    # Drop all columns that have all values missing
    _df.dropna(axis=1, how='all')

    if len(args) == 0:
        logger.warning('No variables were selected, returning all columns{0}'
                       .format(' for level {0}={1}'.format(ix_level, filter_by)
                               if filter_by else ''))
        return _df
    return _df[get_matching_columns(_df, *args, excluded=excluded)]


def _extract_t4csv(file_descriptor):
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

            field_names = SEPARATOR.join(
                data_lines[h_ini:h_last - 1]
            ).split(SEPARATOR)  # This is now a list with all the columns
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

    Arguments:
        t4_csv(str): T4-flavored CSV input filename
        output(str): Plain CSV output filename
    """
    data = reload_from_csv(t4_csv, plain=False)
    data.to_csv(output,
                date_format=T4_DATE_FORMAT)


def dataframe_to_t4csv(dataframe, output, t4format=2):
    """
    Save dataframe to Format1/2 T4-compliant CSV files, one per system

    Arguments:
        dataframe(pandas.DataFrame): Input data
        output(str): T4-flavored CSV output filename
    Keyword Arguments:
        t4format(int [1|2]): T4 format
    Return:
        dict
        Dictionary matching ``{system: filename}``
    """
    output_names = {}
    (_dir, output) = os.path.split(output)
    for system in dataframe.index.levels[1]:
        data = dataframe.xs(system, level='system')
        try:
            buffer_object = cStringIO()
            data.to_csv(buffer_object,
                        date_format=T4_DATE_FORMAT)
            buffer_object.seek(0)
            output_names[system] = '{0}{1}{3}_{2}{4}'.format(
                _dir,
                os.sep if _dir else '',
                system,
                *os.path.splitext(output)
            )
            _to_t4csv(buffer_object,
                      output=output_names[system],
                      t4format=t4format,
                      system_id=system)
        finally:
            buffer_object.close()
    return output_names


def _add_secondary_index(dataframe, system):
    """
    Get a 2-indices dataframe from a dataframe with a DateTimeIndex index
    with (dataframe.DateTimeIndex, system) as the new index
    """
    # Add a secondary index based in the value of `system` in order to avoid
    # breaking cluster statistics, i.e. data coming from cluster LONDON and
    # represented by systems LONDON-1 and LONDON-2
    index_len = len(dataframe.index)
    midx = pd.MultiIndex(
        levels=[dataframe.index.get_level_values(0).values, [system]],
        labels=[list(range(index_len)), [0] * index_len],
        names=[DATETIME_TAG, 'system']
    )
    return dataframe.set_index(midx)


def plain_to_t4csv(plain_csv, output, t4format=2, system=None):
    """
    Convert plain CSV into T4-compliant Format1/2 CSV file

    Arguments:
        plain_csv (str): Plain CSV output filename
        output (str): T4-flavored CSV output filename
    Keyword Arguments:
        t4format (int [1|2]): T4 format
        system (Optional[str]): System to which the input data belongs
    """
    data = reload_from_csv(plain_csv, plain=True)
    if not system:  # if no system, just set the file name as system
        system = os.path.splitext(os.path.basename(plain_csv))[0]
    data = _add_secondary_index(data, system)
    dataframe_to_t4csv(dataframe=data,
                       output=output,
                       t4format=t4format)


def _to_t4csv(file_object, output, t4format=2, system_id=None):
    """
    Save file_object contents to Format1/2 T4-compliant CSV file
    """
    if t4format not in [1, 2]:
        raise AttributeError('Bad T4-CSV format {0} (must be either 1 '
                             'or 2)'.format(t4format))
    with open(output, 'w') as csvfile:
        csvfile.write('{0}, t4Monitor Version: {1}, '
                      'File Type Format {2}\n'.format(
                          system_id or 'SYSTEM',
                          t4mon.__version__,
                          t4format
                      ))
        if t4format == 2:
            csvfile.write('{0}\n'.format(START_HEADER_TAG))
            csvfile.write(file_object.readline())  # Fields in 1st line
            csvfile.write('{0}\n'.format(END_HEADER_TAG))
        csvfile.write(file_object.read())


def to_dataframe(field_names, data):
    """
    Core method used by :func:`~dataframize`.
    Load T4-CSV data into a pandas DataFrame
    """
    _df = pd.DataFrame()  # default to be returned if exception is found
    try:
        if field_names and data:  # else return empty dataframe
            # put data in a file object and send it to pd.read_csv()
            fbuffer = cStringIO()
            fbuffer.writelines(('{0}\n'.format(line) for line in data))
            fbuffer.seek(0)
            # Multiple columns may have a 'sample time' alike column,
            # only use first (case insensitive search)
            df_timecol = (s for s in field_names
                          if DATETIME_TAG.upper() in s.upper())
            index_col = [advance_iterator(df_timecol)]
            _df = pd.read_csv(fbuffer,
                              header=None,
                              parse_dates=index_col,
                              index_col=index_col,
                              names=field_names)
            # Remove redundant time columns (if any)
            _df.drop(df_timecol, axis=1, inplace=True)
            # Remove duplicate columns to avoid problems with combine_first()
            _df = remove_duplicate_columns(_df)

    except (StopIteration, Exception) as exc:  # Not T4-compliant!
        raise ToDfError(exc)

    return _df


def dataframize(data_file, session=None, logger=None):
    """
    Load CSV data into a pandas DataFrame.

    Return an empty DataFrame if fields and data are not correct,
    otherwise it will interpret it with NaN values.

    Column named :const:`DATETIME_TAG` (i.e. 'Sample Time') is used as index.
    It is common in T4 files to have several columns with a sample time, most
    probably as a product of an horizontal merge of different CSVs. In those
    cases the first column having 'Sample Time' on its name will be used.


    If ``session`` is not a valid SFTP session, work with local file system.

    Arguments:
        data_file (str): Input T4-CSV filename
    Keyword Arguments:
        session (Optional[SFTPClient]): Active SFTP session to a remote host
        logger (Optional[logging.Logger]): logging instance
    Return:
        pandas.DataFrame
    """

    logger = logger or init_logger()
    logger.info('Loading file {0}...'.format(data_file))
    try:
        if not isinstance(session, SFTPClient):
            session = builtins  # open local file
        with session.open(data_file) as file_descriptor:
            _single_df = to_dataframe(*_extract_t4csv(file_descriptor))
        return _single_df
    except IOError:  # non-existing files also return an empty dataframe
        logger.error('File not found: {0}'.format(data_file))
        return pd.DataFrame()
    except ExtractCSVException:
        logger.error('An error occurred while extracting the CSV file: {0}'
                     .format(data_file))
        return pd.DataFrame()
    except ToDfError:
        logger.error('Error occurred while processing CSV file: {0}'
                     .format(data_file))
        return pd.DataFrame()


def remove_outliers(dataframe, n_std=2):
    """
    Remove all rows that have outliers in at least one column from a dataframe
    by default larger than n_std standard deviations from the mean, in absolute
    value, by default 2 std.
    """
    return dataframe[dataframe.apply(lambda x:
                                     np.abs(x - x.mean()) <= n_std * x.std()
                                     ).all(axis=1)]
