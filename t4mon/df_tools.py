#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
*t4mon* - T4 monitoring **test functions** for df_tools.py
"""

from __future__ import absolute_import

import gzip
import __builtin__
from re import split
from cStringIO import StringIO
from itertools import takewhile

import numpy as np
import pandas as pd
from paramiko import SFTPClient

from .logger import init_logger

try:
    import cPickle as pickle
except ImportError:
    import pickle


SEPARATOR = ','  # CSV separator, usually a comma
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"  # Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"  # End of Format-2 header
DATETIME_TAG = 'Sample Time'  # Column containing sample datetime


__all__ = ('select_var', 'copy_metadata', 'restore_metadata',
           'extract_df', 'to_dataframe', 'dataframize')


class ToDfError(Exception):

    """Exception raised while converting a CSV into a pandas dataframe"""
    pass


class ExtractCSVException(Exception):

    """Exception raised while extracting a CSV file"""
    pass


def consolidate_data(partial_dataframe, dataframe=None, system=None):
    """
    Consolidates partial_dataframe with dataframe by calling
    df_tools.consolidate_data
    """
    if not system:
        raise ToDfError('Need a system to consolidate the dataframe')
    if dataframe is None:
        dataframe = pd.DataFrame()

    if not isinstance(partial_dataframe, pd.DataFrame):
        raise ToDfError('Cannot consolidate with a non-dataframe object')

    # Overwrite system column to avoid breaking cluster statistics,
    # i.e. data coming from cluster LONDON and represented by systems
    # LONDON_1 and LONDON_2
    system_column = get_column_name_case_insensitive(partial_dataframe,
                                                     'system')
    partial_dataframe[system_column] = system
    # dataframe = dataframe.combine_first(partial_dataframe)
    # dataframe = pd.concat([dataframe, partial_dataframe])
    return dataframe.combine_first(partial_dataframe)


def remove_dataframe_holes(dataframe):
    """ Concatenate partial dataframe with resulting dataframe
    """
    # Group by index while keeping the metadata
    tmp_meta = copy_metadata(dataframe)
    dataframe = dataframe.groupby(dataframe.index).last()
    restore_metadata(tmp_meta, dataframe)
    return dataframe


def metadata_from_cols(data):
    """
    Restores metadata from CSV, where metadata was saved as extra columns
    """
    for item in data._metadata:
        metadata_values = np.unique(data[item])
        setattr(data, item, set(metadata_values))


def metadata_to_cols(dataframe, metadata):
    """
    Synthesize additional columns based in metadata values and stores the
    metadata inside the (modified) dataframe object
    """
    for item in metadata:
        setattr(dataframe, item, metadata[item])
        dataframe[item] = pd.Series([metadata[item]]*len(dataframe),
                                    index=dataframe.index)
        if item not in dataframe._metadata:
            dataframe._metadata.append(item)


def reload_from_csv(csv_filename, plain=False):
    """
    Load a CSV into a dataframe and synthesize its metadata
    Assumes that first column contains the timestamp information
    """
    if plain:  # plain CSV
        data = pd.read_csv(csv_filename, index_col=0)
    else:
        data = dataframize(csv_filename)
    data.index = pd.to_datetime(data.index)
    if plain:  # Restore the index name
        data.index.name = 'datetime'
    metadata_from_cols(data)  # restore metadata fields
    return data


def get_matching_columns(dataframe, var_names):
    """ Filter column names that match first item in var_names, which can
        have wildcards ('*'), like 'str1*str2'; in that case the column
        name must contain both 'str1' and 'str2'. """
    if dataframe.empty:
        return []
    else:
        return [col for col in dataframe.columns
                for var_item in var_names
                if all([k in col.upper() for k in
                        var_item.upper().strip().split('*')])]


def get_column_name_case_insensitive(dataframe, name):
    """
    Returns the actual column name from dataframe where dataframe.column.values
    matches name in case-insensitive
    """
    if name and not dataframe.empty:
        colnames = [colname.upper() for colname in dataframe.columns
                    if colname]
        name = name.upper()
        if name in colnames:
            return dataframe.columns[colnames.index(name)]
    return None


def select_var(dataframe, *var_names, **optional):
    """
    Returns selected variables that match columns from the dataframe.

    var_names: Filter column names that match any var_names; each individual
               var_item in var_names (1st one only if not filtering on system)
               can have wildcards ('*') like 'str1*str2'; in that case the
               column name must contain both 'str1' and 'str2'.
    optional: - split_by (filter or not based on that column name and content),
                only one filter allowed. Example: system='SYSTEM1'
              - logger (logging.Logger instance)
    """
    logger = optional.pop('logger', '') or init_logger()
    (column_filter, filter_by) = optional.popitem() if optional else (None,
                                                                      None)
    # Work with a case insensitive copy of the column names
    column_name = get_column_name_case_insensitive(dataframe, column_filter)
    if column_filter and not column_name:
        logger.warning('Bad filter found: %s not found (case insensitive)',
                       column_filter)
        filter_by = None

    if not filter_by and len(var_names) > 1:
        logger.warning('Only first match will be extracted when no filter '
                       'is applied: %s', var_names[0])
        var_names = var_names[0:1]

    if filter_by:
        # logger.debug('Filtering by %s=%s', column_name, filter_by)
        # case-insensitive search
        my_filter = [k.upper() == filter_by.upper()
                     for k in dataframe[column_name]]
    else:
        my_filter = dataframe.columns
    if len(var_names) == 0:
        logger.warning('No variables were selected, returning all '
                       'columns %s',
                       'for filter {}={}'.format(column_name, filter_by)
                       if filter_by else '')
        return dataframe[my_filter].dropna(axis=1, how='all').columns

    return get_matching_columns(dataframe[my_filter].dropna(axis=1,
                                                            how='all'),
                                var_names)


def extract_df(dataframe, *var_names, **kwargs):
    """
    Returns dataframe which columns meet the criteria:

     - When a system is selected, return all columns whose names have(not case
       sensitive) var_names on it: COLUMN_NAME == *VAR_NAMES* (wildmarked)

     - When no system is selected, work only with the first element of
       var_names and return: COLUMN_NAME == *VAR_NAMES[0]* (wildmarked)

    """
    logger = kwargs.pop('logger') if 'logger' in kwargs else init_logger()
    if dataframe.empty:
        return dataframe
    (col_name, row_filter) = kwargs.iteritems().next() if kwargs \
        else (None, None)
    selected_columns = select_var(dataframe,
                                  *var_names,
                                  logger=logger,
                                  **kwargs)
    if len(selected_columns):
        if row_filter:
            groupped = dataframe.groupby(col_name)
            row_filter = (k for k in groupped.groups.keys()
                          if k.upper() == row_filter.upper()).next()
            return groupped.get_group(row_filter)
        else:
            return dataframe[selected_columns]
    else:
        return pd.DataFrame()


def copy_metadata(source):
    """ Copies metadata from source columns to a list of dictionaries of type
        [{('column name', key): value}]
    """
    assert isinstance(source, pd.DataFrame)
    return dict([((key), getattr(source, key, ''))
                 for key in source._metadata])


def restore_metadata(metadata, dataframe):
    """ Restores previously retrieved metadata into the dataframe
    """
    assert isinstance(metadata, dict)
    assert isinstance(dataframe, pd.DataFrame)
    for keyvalue in metadata:
        setattr(dataframe, keyvalue, metadata[keyvalue])
        if keyvalue not in dataframe._metadata:
            dataframe._metadata.append(keyvalue)


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
        # Add fake columns based in metadata
        metadata_to_cols(_df, metadata)
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


def to_pickle(self, name, compress=False):
    """ Allow saving metadata to gzipped pickle """
    buffer_object = StringIO()
    pickle.dump(self, buffer_object, protocol=pickle.HIGHEST_PROTOCOL)
    pickle.dump(self._metadata,
                buffer_object,
                protocol=pickle.HIGHEST_PROTOCOL)
    for item in self._metadata:
        pickle.dump(getattr(self, item),
                    buffer_object,
                    protocol=pickle.HIGHEST_PROTOCOL)
    buffer_object.flush()
    if name.endswith('.gz'):
        compress = True
        name = name.rsplit('.gz')[0]  # we're appending the gz extensions below

    if compress:
        output = gzip
        name = "%s.gz" % name
    else:
        output = __builtin__

    with output.open(name, 'wb') as pkl_out:
        pkl_out.write(buffer_object.getvalue())
    buffer_object.close()


def read_pickle(name, compress=False):
    """ Properly restore dataframe plus its metadata from pickle store """
    if compress or name.endswith('.gz'):
        mode = gzip
    else:
        mode = __builtin__

    with mode.open(name, 'rb') as picklein:
        try:
            dataframe = pickle.load(picklein)
            setattr(dataframe, '_metadata', pickle.load(picklein))
            for item in dataframe._metadata:
                setattr(dataframe, item, pickle.load(picklein))
        except EOFError:
            pass
        return dataframe if 'dataframe' in locals() else pd.DataFrame()
