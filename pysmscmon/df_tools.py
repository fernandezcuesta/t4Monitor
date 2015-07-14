#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:11:38 2015

@author: fernandezjm
"""
from __future__ import absolute_import
import __builtin__
import pandas as pd
import numpy as np
import gzip

try:
    import cPickle as pickle
except ImportError:
    import pickle

from itertools import takewhile
from re import split
from cStringIO import StringIO
from paramiko import SFTPClient

from .logger import init_logger

SEPARATOR = ','                                # CSV separator, usually a comma
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"    # Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"          # End of Format-2 header
DATETIME_TAG = 'Sample Time'                # Column containing sample datetime


__all__ = ('select_var', 'copy_metadata', 'restore_metadata',
           'extract_df', 'to_dataframe', 'dataframize')


class ToDfError(Exception):

    """Exception raised while converting a CSV into a pandas dataframe"""
    pass


class ExtractCSVException(Exception):

    """Exception raised while extracting a CSV file"""
    pass


def consolidate_data(data, tmp_data=None):
    """ Concatenate partial dataframe with resulting dataframe
    """
    if isinstance(tmp_data, pd.DataFrame) and not tmp_data.empty:
        data = pd.concat([data, tmp_data])
    # Group by index while keeping the metadata
    tmp_meta = copy_metadata(data)
    data = data.groupby(data.index).last()
    restore_metadata(tmp_meta, data)
    if isinstance(data.system, set):
        # we are only interested in first 5 chars of the system name
        data.system = set([i[0:5] for i in data.system])
    return data


def metadata_from_cols(data):
    """
    Restores metadata from CSV, where metadata was saved as extra columns
    """
    for item in data._metadata:
        metadata_values = np.unique(data[item])
        if len(metadata_values) > 1:
            setattr(data, item, set(metadata_values))
        else:
            setattr(data, item, metadata_values[0])


def reload_from_csv(csv_filename):
    """ Load a CSV into a dataframe and synthesize its metadata """
    data = pd.read_csv(csv_filename)

    metadata_from_cols(data)  # restore metadata fields
    if 'datetime' in data:
        return data.set_index('datetime')
    else:
        return data


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
    (column_name, column_filter) = optional.popitem() if optional else (None,
                                                                        None)
    # Work with a case insensitive copy of the column names
    colnames = [colname.upper() for colname in dataframe.columns]
    if column_name:
        column_name = column_name.upper()
        if column_name in colnames:
            column_index = dataframe.columns[colnames.index(column_name)]
        else:
            logger.warning('Bad filter found: %s not found (case insensitive)',
                           column_name)
            column_name = column_filter = None

    def get_matching_columns(dataframe):
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

    if not column_filter and len(var_names) > 1:
        logger.warning('Only first match will be extracted when no filter '
                       'is applied: %s', var_names[0])
        var_names = var_names[0:1]

    if column_filter:
        my_filter = [k == column_filter
                     for k in dataframe[column_index]]
    else:
        my_filter = dataframe.columns
    if len(var_names) == 0:
        logger.warning('No variables were selected, returning all '
                       'columns %s',
                       'for filter {}={}'.format(column_index, column_filter)
                       if column_filter else '')
        return dataframe[my_filter].dropna(axis=1, how='all').columns
    return get_matching_columns(dataframe[my_filter].dropna(axis=1,
                                                            how='all'))


def extract_df(dataframe, *var_names, **kwargs):
    """
    Returns dataframe which columns meet the criteria:
    - When a system is selected, return all columns whose names have(not case
    sensitive) var_names on it: COLUMN_NAME == *VAR_NAMES* (wildmarked)
    - When no system is selected, work only with the first element of var_names
    and return: COLUMN_NAME == *VAR_NAMES[0]* (wildmarked)
    """
    logger = kwargs.pop('logger') or init_logger()
    if dataframe.empty:
        return dataframe
    col_name, col_filter = kwargs.iteritems().next() if kwargs \
        else (None, None)
    selected = select_var(dataframe,
                          *var_names,
                          logger=logger,
                          **kwargs)
    if len(selected):
        if col_filter:
            return dataframe[dataframe[col_name] == col_filter][selected]
        else:
            return dataframe[selected]
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
         * header:     List of strings (column names)
         * data_lines: List of strings (each one representing a sample)
         * metadata:   Cluster name as found in the first line of Format1/2 CSV
    """
    try:
        data_lines = [li.rstrip()
                      for li in takewhile(lambda x:
                                          not x.startswith('Column Average'),
                                          file_descriptor)]
        _l0 = split(r'/|%c *| *' % SEPARATOR, data_lines[0])
        metadata = {'system': _l0[1] if _l0[0] == 'Merged' else _l0[0]}
        if data_lines[1].find(START_HEADER_TAG):  # Format 1
            header = data_lines[3].split(SEPARATOR)
            data_lines = data_lines[4:]
        else:  # Format 2
            h_last = data_lines.index(END_HEADER_TAG)
            header = SEPARATOR.join(data_lines[2:h_last]).split(SEPARATOR)
            data_lines = data_lines[h_last + 1:]
        return (header, data_lines, metadata)
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
        for item in metadata:
            setattr(_df, item, metadata[item])
            _df[item] = pd.Series([metadata[item]]*len(_df), index=_df.index)
            if item not in _df._metadata:
                _df._metadata.append(item)
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
