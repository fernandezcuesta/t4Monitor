#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 pySMSCMon: T4-compliant CSV processor and visualizer for Acision SMSC Monitor
 ------------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 Syntax:

 t4 input_file

 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information.

  line1: Collection date  (optional line)

  line2: Start time       (optional line)

  line3: Parameter Headings (comma separated).

 or

  ** Format 2: **

  line0: Header information containing T4 revision info and system information.

  line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple $

  line2: parameter headings (comma separated)
   ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple $

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing hash
  and contents followed by it (sometimes even more samples...) is ignored.

"""

# Set backend before first import of pyplot or pylab
import os
import matplotlib
# Set matplotlib's backend, Qt4 doesn't like threads
if os.name == 'posix':
    matplotlib.use('Cairo')
else:
    matplotlib.use('TkAgg')
# Required by matplotlib when using TkAgg backend
#    import FileDialog
from matplotlib import pyplot as plt, dates as md
import ConfigParser
import calculations
import sshtunnel
import pandas as pd
import threading
import datetime as dt
import Queue as queue
import logging
import sys
import getpass
from sftpsession import SftpSession, SFTPSessionError
from cStringIO import StringIO
from itertools import takewhile
from re import split
from random import randint
from paramiko import SSHException

__version_info__ = (0, 5, 2)
__version__ = '.'.join(str(i) for i in __version_info__)
__author__ = 'fernandezjm'

__all__ = ('select_var', 'main', 'plot_var', 'copy_metadata', 'to_base64',
           'get_stats_from_host', 'restore_metadata', 'extract_df')


# CONSTANTS
DEFAULT_LOGLEVEL = 'INFO'
SETTINGS_FILE = 'settings.cfg'
START_HEADER_TAG = "$$$ START COLUMN HEADERS $$$"    # Start of Format-2 header
END_HEADER_TAG = "$$$ END COLUMN HEADERS $$$"          # End of Format-2 header
DATETIME_TAG = 'Sample Time'                # Column containing sample datetime
SEPARATOR = ','                                # CSV separator, usually a comma
# Avoid using locale in Linux+Windows environments, keep these lowercase
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
LINE = 80 * '-'
LOG_COMMAND = '@smsc$root:[monitor]log_mon_onscreen'


# CLASSES
class ExtractCSVException(Exception):

    """Exception raised while extracting a CSV file"""
    pass


class ConfigReadError(Exception):

    """Exception raised while reading configuration file"""
    pass


class ToDfError(Exception):

    """Exception raised while converting a CSV into a pandas dataframe"""
    pass


class InitTunnelError(Exception):

    """Exception raised by InitTunnel"""
    pass


class NoFileFound(Exception):

    """Exception raised by get_stats_from_host"""
    pass


class SData(object):

    """
    Defines class 'SData' for use in `get_system_data` and `thread_wrapper`
    """

    def __init__(self):
        self.server = None
        self.system = ''
        self.conf = None
        self.alldays = False
        self.nologs = False
        self.logger = None

    def clone(self, system):
        """ Makes a copy of SData
        """
        my_clone = SData()
        my_clone.server = self.server
        my_clone.system = system
        my_clone.conf = self.conf
        my_clone.alldays = self.alldays
        my_clone.nologs = self.nologs
        my_clone.logger = self.logger
        return my_clone


# ADD METHODS TO PANDAS DATAFRAME
def _custom_finalize(self, other, method=None):
    """ As explained in http://stackoverflow.com/questions/23200524/
        propagate-pandas-series-metadata-through-joins
        => Custom __finalize__ function for concat, so we keep the metadata
    """
    def _wrapper(element, name):
        """ Wrapper for map function """
        _cur_meta = getattr(self, name, '')
        _el_meta = getattr(element, name, '')
        if _cur_meta:
            if isinstance(_el_meta, set):
                object.__setattr__(self, name, _cur_meta.union(_el_meta))
            else:
                _cur_meta.add(_el_meta)
        else:
            object.__setattr__(self, name,
                               _el_meta if isinstance(_el_meta, set)
                               else set([_el_meta]))
    for name in self._metadata:
        if method == 'concat':
            map(lambda element: _wrapper(element, name), other.objs)
        else:
            object.__setattr__(self, name, getattr(other, name, ''))
    return self


pd.DataFrame._metadata = ['system']
pd.DataFrame.__finalize__ = _custom_finalize
pd.DataFrame.oper = calculations.oper
pd.DataFrame.oper_wrapper = calculations.oper_wrapper
pd.DataFrame.recursive_lis = calculations.recursive_lis
pd.DataFrame.apply_lis = calculations.apply_lis
# END OF ADD METHODS TO PANDAS DATAFRAME


def select_var(dataframe, *var_names, **optional):
    """
    Yields selected variables that match columns from the dataframe.
    var_names: Filter column names that match any var_names; each individual
               var_item in var_names (first one if we also filter on system)
               can have wildcards ('*') like 'str1*str2'; in that case the
               column name must contain both 'str1' and 'str2'.
    optional: system (filter or not based on the system)
              logger (logging.Logger instance)
    """
    logger = optional.get('logger', '') or init_logger()
    system_filter = optional.get('system', '').upper()
    if 'system' not in dataframe:
        dataframe['system'] = 'no-system'

    if system_filter:
        if len(var_names) > 0:
            # Filter column names that match any var_names;
            # each individual var_item in var_names can have wildcards ('*')
            # like 'str1*str2'; in that case the column name must contain both
            # 'str1' and 'str2'.
            # Dropping all columns where all items are NA (axis=1, how='all').
            selected = [s for s in dataframe.dropna(axis=1, how='all').columns
                        for var_item in var_names
                        if all([k in s.upper()
                                for k in var_item.upper().strip().split('*')])]
        else:
            selected = dataframe.columns
        if not selected:  # if var_names were not found in dataframe columns
            logger.warning('%s| %s not found for this system, '
                           'nothing selected.', system_filter, var_names)
        yield selected
    else:  # no system selected, work only with first variable for all systems
        if len(var_names) == 0:
            logger.warning('No variables were selected, returning all columns')
            yield dataframe.columns
        my_var = var_names[0].upper()
        if len(var_names) > 1:
            logger.warning('Only first match will be extracted when no system '
                           'is selected: %s', my_var)

        for _, grp in dataframe.groupby(['system']):
            # Filter column names that match first item in var_names, which can
            # have wildcards ('*'), like 'str1*str2'; in that case the column
            # name must contain both 'str1' and 'str2'.
            selected = [s for s in grp.columns
                        if all([k in s.upper() for k in
                                my_var.strip().split('*')])]
            if selected:
                yield selected
            else:
                logger.warning('%s not found for system/s: %s, nothing was '
                               'selected.',
                               var_names[0],
                               [str(item) for item in set(dataframe.system)])
                yield []


def extract_df(dataframe, *var_names, **optional):
    """
    Returns dataframe which columns meet the criteria:
    - When a system is selected, return all columns whose names have(not case
    sensitive) var_names on it: COLUMN_NAME == *VAR_NAMES* (wildmarked)
    - When no system is selected, work only with the first element of var_names
    and return: COLUMN_NAME == *VAR_NAMES[0]* (wildmarked)
    """
    logger = optional.get('logger', '') or init_logger()
    if dataframe.empty:
        return dataframe
    system_filter = optional.get('system', '').upper()
    selected = select_var(dataframe,
                          *var_names,
                          system=system_filter,
                          logger=logger)
    if system_filter:
        sel_list = list(*selected)
        if sel_list:
            _df = dataframe[dataframe['system'] == system_filter][sel_list]
#            _df['system'] = system_filter
            _df['system'] = pd.Series([system_filter]*len(_df),
                                      index=_df.index)
        else:
            _df = pd.DataFrame()
    else:
        for _, grp in dataframe.groupby(['system']):
            sel_list = list(selected.next())
            if sel_list:
                # Filterer column names that match first item in var_names,
                # which can have wildcards ('*'), like 'str1*str2'; in that
                # case the column name must contain both 'str1' and 'str2'.
                _df = pd.concat([_df, grp[sel_list]]) \
                      if '_df' in locals() else grp[sel_list]
            else:
                _df = pd.DataFrame()
    return _df


def plot_var(dataframe, *var_names, **optional):
    """
    Plots the specified variable names from the dataframe overlaying
    all plots for each variable and silently skipping unexisting variables.

    - Optionally selects which system to filter on (i.e. system='localhost')
    - Optionally sends keyworded parameters to pyplot (**optional)

    var_names: Filter column names that match any var_names; each individual
               var_item in var_names (first one if we also filter on system)
               can have wildcards ('*') like 'str1*str2'; in that case the
               column name must contain both 'str1' and 'str2'.
    """
    logger = optional.pop('logger', '') or init_logger()
    if 'system' not in dataframe:
        dataframe['system'] = 'no-system'

    try:
        if dataframe.empty:
            raise TypeError

        system_filter = optional.pop('system', '').upper()
        selected = select_var(dataframe,
                              *var_names,
                              system=system_filter,
                              logger=logger)
        if system_filter:
            sel = list(*selected)
            if not sel:
                raise TypeError
            plotaxis = dataframe[dataframe['system'] == system_filter][sel].\
                        dropna(axis=1, how='all').plot(**optional)
        else:
            plt.set_cmap(optional.pop('cmap',
                                      optional.pop('colormap', 'Reds')))
            optional['title'] = optional.pop('title', var_names[0].upper())
            plotaxis = plt.gca()
            for key in optional:
                # eval('plt.%s(optional[key])' % key)
                getattr(plt, key)(optional[key])

            for key, grp in dataframe.groupby(['system']):
                sel_list = list(selected.next())
                if not sel_list:
                    # other systems may have this
                    continue
                for item in sel_list:
                    logger.debug('Drawing item: %s (%s)' % (item, key))
                    # convert timestamp to number
                    my_ts = [ts.to_julian_date() - 1721424.5
                             for ts in grp[item].dropna().index]
                    plt.plot(my_ts,
                             grp[item].dropna(), label='%s@%s' % (item, key))
            if not sel_list:  # nothing at all was found
                raise TypeError
        # Style the resulting plot
        plotaxis.xaxis.set_major_formatter(md.DateFormatter('%d/%m/%y\n%H:%M'))
        plotaxis.legend(loc='best')
        # rstyle(plotaxis)
        return plotaxis

    except TypeError:
        logger.error('%s%s not drawn%s',
                     '{}| '.format(system_filter) if system_filter else '',
                     var_names,
                     ' for this system' if system_filter else '')
        _ = plt.plot()
        return plt.gca()
    except Exception as exc:
        _, _, exc_tb = sys.exc_info()
        logger.error('Exception at plot_var (line %s): %s',
                     exc_tb.tb_lineno,
                     repr(exc))


def to_base64(dataframe_plot):
    """
    Converts a plot into base64-encoded graph
    """
    try:
        if not dataframe_plot.has_data():
            raise AttributeError
        fbuffer = StringIO()
        fig = dataframe_plot.get_figure()
        fig.savefig(fbuffer, format='png', bbox_inches='tight')
        plt.close()
        fbuffer.seek(0)
        return 'data:image/png;base64,' + fbuffer.getvalue().encode("base64")
    except AttributeError:
        return ''


def copy_metadata(source):
    """ Copies metadata from source columns to a list of dictionaries of type
        [{('column name', key): value}]
    """
    assert isinstance(source, pd.DataFrame)
    return dict([((key), getattr(source, key, ''))
                 for key in source._metadata])


def restore_metadata(metadata, dataframe):
    """ Restores previously retrieved metadata into the dataframe
        It is assumed that metadata was taken from a dataframe with same size
    """
    assert isinstance(metadata, dict)
    assert isinstance(dataframe, pd.DataFrame)
    for kv in metadata:
        object.__setattr__(dataframe, kv, metadata[kv])
        if kv not in dataframe._metadata:
            dataframe._metadata.append(kv)
    return dataframe


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
    _df = pd.DataFrame()
    try:
        fbuffer = StringIO()
        for i in data:
            fbuffer.write('%s\n' % i)
        fbuffer.seek(0)
        if field_names and data:  # else return empty dataframe
            # Multiple columns may have a sample time, parse dates from all
            df_timecol = [s for s in field_names if DATETIME_TAG in s][0]
            if df_timecol == '':
                raise ToDfError
            _df = pd.read_csv(fbuffer, names=field_names,
                              parse_dates={'datetime': [df_timecol]},
                              index_col='datetime')
        for item in metadata:
#            _df[item] = metadata[item]
            _df[item] = pd.Series([metadata[item]]*len(_df), index=_df.index)
            object.__setattr__(_df, item, metadata[item])
            if item not in _df._metadata:
                _df._metadata.append(item)
    except Exception as exc:
        raise ToDfError(exc)
    return _df


def dataframize(a_file, sftp_session, logger=None):
    """
    Wrapper for to_dataframe, leading with non-existing files over sftp
    """

    logger = logger or init_logger()
    logger.info('Loading file %s...', a_file)
    try:
        with sftp_session.open(a_file) as file_descriptor:
            _single_df = to_dataframe(*extract_t4csv(file_descriptor))
        return _single_df
    except IOError:  # non-existing files also return an empty dataframe
        return pd.DataFrame()
    except ExtractCSVException:
        logger.error('An error occured while extracting the CSV file: %s',
                     a_file)
        return pd.DataFrame()
    except ToDfError:
        logger.error('Error occurred while internally processing CSV file: %s',
                     a_file)
        return pd.DataFrame()
#    except Exception:
#        exc_typ, _, exc_tb = sys.exc_info()
#        print 'Unexpected error ({2}@line {0}): {1}'.format(exc_tb.tb_lineno,
#                                 exc_typ, exc_tb.tb_frame.f_code.co_filename)


def get_stats_from_host(hostname, *files, **kwargs):
    """
    Connects to a remote system via SFTP and reads the CSV files, then calls
    the csv-pandas conversion function.
    Returns: pandas dataframe

     **kwargs (optional):
    sftp_client: already established sftp session
    logger: logging.Logger instance
    ssh_user, ssh_pass, ssh_pkey_file, ssh_configfile, ssh_port
    Otherwise: checks ~/.ssh/config
    """
    logger = kwargs.get('logger', '') or init_logger()
    sftp_session = kwargs.pop('sftp_client', '')

    _df = pd.DataFrame()

    if not files:
        logger.debug('Nothing gathered from %s, no files were selected',
                     hostname)
        return _df
    try:
        if not sftp_session:
            session = SftpSession(hostname, **kwargs).connect()
            if not session:
                logger.debug('Could not establish an SFTP session to %s',
                             hostname)
                return pd.DataFrame()
            sftp_session = session.sftp_session
            close_me = True
        else:
            # repeating same code with the sftp_client passed to the function
            logger.debug('Using established sftp session...')
            close_me = False

        _df = pd.concat([dataframize(a_file, sftp_session, logger)
                         for a_file in files], axis=0)
        if close_me:
            logger.debug('Closing sftp session')
            session.close()

        _tmp_df = copy_metadata(_df)
#       properly merge the columns and restore the metadata
        _df = _df.groupby(_df.index).last()
        restore_metadata(_tmp_df, _df)

    except SFTPSessionError as _exc:
        logger.error('Error occurred while SFTP session to %s: %s',
                     hostname,
                     _exc)
#    except Exception:
#        exc_typ, _, exc_tb = sys.exc_info()
#        print 'Unexpected error ({2}@line {0}): {1}'.format(exc_tb.tb_lineno,
#                                 exc_typ, exc_tb.tb_frame.f_code.co_filename)
    finally:
        return _df


def read_config(*settings_file):
    """ Return ConfigParser object from configuration file """
    config = ConfigParser.SafeConfigParser()
    try:
        settings = config.read(settings_file) if settings_file \
                                              else config.read(SETTINGS_FILE)
    except ConfigParser.Error as _exc:
        raise ConfigReadError(repr(_exc))

    if not settings or not config.sections():
        raise ConfigReadError('Could not read configuration %s!' %
                              settings_file if settings_file
                              else SETTINGS_FILE)
    # If no 'username' was set in DEFAULT, override with current user's
    try:
        username = config.get('DEFAULT', 'username')
        if not username:
            raise ConfigParser.NoOptionError()
    except ConfigParser.NoOptionError:
        config.set('DEFAULT', 'username', getpass.getuser())
    return config


def init_tunnels(config, logger, system=None):
    """
    Calls sshtunnel and returns a ssh server with all tunnels established
    server instance is returned non-started
    """
    if not config:
        config = read_config()

    jumpbox_addr = config.get('GATEWAY', 'ip_or_hostname')
    jumpbox_port = int(config.get('GATEWAY', 'ssh_port'))
    rbal = []
    lbal = []
    tunnelports = {}

    for _sys in [system] if system else [x for x in config.sections()
                                         if x not in ['GATEWAY', 'MISC']]:
        rbal.append((config.get(_sys, 'ip_or_hostname'),
                     int(config.get(_sys, 'ssh_port'))))
        lbal.append(('', int(config.get(_sys, 'tunnel_port')) or
                     randint(20000, 50000)))  # if local port==0, random port
        tunnelports[_sys] = lbal[-1][-1]
        config.set(_sys, 'tunnel_port', str(tunnelports[_sys]))
    try:
        # Assert local tunnel ports are different
        assert len(tunnelports) == len(set([tunnelports[k]
                                            for k in tunnelports]))
        pwd = config.get('GATEWAY', 'password').strip("\"' ") or None \
            if config.has_option('GATEWAY', 'password') else None
        pkey = config.get('GATEWAY', 'identity_file').strip("\"' ") or None \
            if config.has_option('GATEWAY', 'identity_file') else None

        server = sshtunnel.SSHTunnelForwarder(ssh_address=jumpbox_addr,
                                              ssh_port=jumpbox_port,
                                              ssh_username=config.
                                              get('GATEWAY', 'username'),
                                              ssh_password=pwd or None,
                                              remote_bind_address_list=rbal,
                                              local_bind_address_list=lbal,
                                              threaded=False,
                                              logger=logger,
                                              ssh_private_key_file=pkey or None
                                              )
        # Add the system<>port bindings to the return object
        server.tunnelports = tunnelports
        return server
    except AssertionError:
        logger.error('Local tunnel ports MUST be different: %s', tunnelports)
        raise sshtunnel.BaseSSHTunnelForwarderError
    except sshtunnel.BaseSSHTunnelForwarderError:
        logger.error('%sCould not open connection to remote server: %s:%s',
                     '%s| ' % system if system else '',
                     jumpbox_addr,
                     jumpbox_port)
        raise sshtunnel.BaseSSHTunnelForwarderError


def get_system_logs(sshsession, system, logger=None):
    """ Get log info from the remote system, assumes an already established
        ssh tunnel.
    """
    logger = logger or init_logger()
    logger.info('Getting log output from %s, may take a while...', system)
    # ignoring stdin and stderr

    try:
        (_, stdout, _) = sshsession.\
                         exec_command(LOG_COMMAND)
#       #remove carriage returns ('\r\n') from the obtained lines
#       logs = [_con for _con in \
#              [_lin.strip() for _lin in stdout.readlines()] if _con]
        return stdout.readlines()  # [_lin for _lin in stdout.readlines()]
    except Exception as _exc:
        logger.error('%s| Error occurred while getting logs: %s',
                     system, repr(_exc))
        return None


def get_system_data(sdata):
    """ Open an sftp session to system and collects the CSVs, generating a
        pandas dataframe as outcome
    """
    data = pd.DataFrame()
    logger = sdata.logger or init_logger()
    system_addr = sdata.conf.get(sdata.system, 'ip_or_hostname')
    tunn_port = sdata.conf.get(sdata.system, 'tunnel_port')
    logger.info('%s| Connecting to tunel port %s', sdata.system, tunn_port)

    ssh_pass = sdata.conf.get(sdata.system, 'password').strip("\"' ") or None \
        if sdata.conf.has_option(sdata.system, 'password') else None
    ssh_key = sdata.conf.get(sdata.system, 'identity_file').strip("\"' ") \
        or None if sdata.conf.has_option(sdata.system, 'identity_file') \
        else None
    with SftpSession(hostname='127.0.0.1',
                     ssh_user=sdata.conf.get(sdata.system, 'username'),
                     ssh_pass=ssh_pass,
                     ssh_key=ssh_key,
                     ssh_timeout=sdata.conf.get(sdata.system, 'ssh_timeout'),
                     ssh_port=tunn_port, logger=logger) as session:

        sftp_session = session.sftp_session

        if not sftp_session:
            raise SftpSession.Break  # break the with statement

        try:
            sftp_session.chdir(sdata.conf.get(sdata.system, 'folder'))
        except IOError:
            logger.error('%s| %s not found at destination',
                         sdata.system,
                         sdata.conf.get(sdata.system, 'folder'))
            raise IOError

        # filter remote files on extension and date
        # using MONTHS to avoid problems with locale rather than english
        # under windows environments
        if sdata.alldays:
            taglist = ['.csv']
        else:
            taglist = ['.csv', '%02i%s%i' % (dt.date.today().day,
                                             MONTHS[dt.date.today().month - 1],
                                             dt.date.today().year)]
        try:  # if present, also filter on cluster id
            taglist.append(sdata.conf.get(sdata.system, 'cluster_id').lower())
        except Exception:
            pass
        # get file list by filtering with taglist (case insensitive)
        file_list = [file_name for file_name in sftp_session.listdir()
                     if all([val in file_name.lower() for val in taglist])]
        if file_list:
            data = get_stats_from_host(system_addr,
                                       *file_list,
                                       sftp_client=sftp_session,
                                       logger=logger)
            if data.empty:
                logger.warning('%s| Data size obtained is 0 Bytes, skipping '
                               'log collection.', sdata.system)
                sdata.__setattr__('nologs', True)
            else:
                logger.info('%s| Data size obtained: %s. '
                            'Now applying calculations...',
                            sdata.system, data.shape)
                data.apply_lis(sdata.conf.get('MISC', 'calculations_file'),
                               logger)
                logger.info('%s| Resulting dataframe size after calculations '
                            'is: %s', sdata.system, data.shape)
        else:
            logger.warning("%s| No files were found at destination (%s@%s:%s) "
                           "with the matching criteria: %s",
                           sdata.system,
                           sdata.conf.get(sdata.system, 'username'),
                           system_addr,
                           sdata.conf.get(sdata.system, 'folder'),
                           taglist)
            logger.warning('%s| Skipping log collection for this system',
                           sdata.system)
            sdata.__setattr__('nologs', True)

        # Done gathering data, now get the logs
        if sdata.nologs:
            logs = '{0}| Log collection omitted'.format(sdata.system)
            logger.info(logs)
        else:
            logs = get_system_logs(session, sdata.system, logger) \
                   or '{}| Missing logs!'.format(sdata.system)
    return data, logs


def init_logger(loglevel=None, name=__name__):
    """ Initialize logger, sets the appropriate level and attaches a console
        handler.
    """
    logger = logging.getLogger(name)
    logger.setLevel(loglevel or DEFAULT_LOGLEVEL)

    # If no console handlers yet, add a new one
    if not any(isinstance(x, logging.StreamHandler) for x in logger.handlers):
        console_handler = logging.StreamHandler()
        if logging.getLevelName(logger.level) == 'DEBUG':
            _fmt = '%(asctime)s| %(levelname)-4.3s|%(threadName)10.9s/' \
                   '%(lineno)04d@%(module)-10.9s| %(message)s'
            console_handler.setFormatter(logging.Formatter(_fmt))
        else:
            console_handler.setFormatter(\
            logging.Formatter('%(asctime)s| %(levelname)-8s| %(message)s'))
        logger.addHandler(console_handler)

    logger.info('Initialized logger with level: %s',
                logging.getLevelName(logger.level))
    return logger


def main(alldays=False, nologs=False, logger=None):
    """ Here comes the main function
    Optional: alldays (Boolean): if true, do not filter on today's date
              nologs (Boolean): if true, skip log info collection
    """
    logger = logger or init_logger()

    _sd = SData()
    _sd.__setattr__('logger', logger)
    _sd.__setattr__('alldays', alldays)
    _sd.__setattr__('nologs', nologs)

    data = pd.DataFrame()
    logs = {}

    # Initialize tunnel(s) and get the SSH session object
    logger.info('Initializing...')

    try:
        _sd.__setattr__('conf', read_config())
        for _sd.system in \
        [x for x in _sd.conf.sections() if x not in ['GATEWAY', 'MISC']]:
            logger.info('%s| Collecting statistics', _sd.system)
            try:
                # server = init_tunnel_per_sys(_sd.conf, system)
                _sd.__setattr__('server',
                                init_tunnels(_sd.conf, logger, _sd.system))
#                _sd.server._tunnel_is_up[_sd.conf.get(_sd.system,
#                                                      'tunnel_port')] = False
            except sshtunnel.BaseSSHTunnelForwarderError:
                continue
            except Exception as _ex:
                logger.error('Unexpected error while opening SSH tunnels')
                logger.error(repr(_ex))
                return data, logs
            logger.info('Opening connection to tunnels')
            _sd.server.start()
#            time.sleep(1)
            # Get data from the remote system
            try:
                tunnelport = _sd.server.tunnelports[_sd.system]
                if not _sd.server.tunnel_is_up[tunnelport]:
                    logger.error('Cannot download data from %s.', _sd.system)
                    raise IOError
                tmp_data, logs[_sd.system] = get_system_data(_sd)
            except (IOError, SFTPSessionError):
                _sd.server.stop()
                logger.warning('Continue to next system')
                continue

            # concatenate with the dataframe to be returned as result
            data = pd.concat([data, tmp_data])

            # Group by index while keeping the metadata
            tmp_meta = copy_metadata(data)
            data = data.groupby(data.index).last()
            restore_metadata(tmp_meta, data)
            # we are only interested in first 5 chars of the system name
            data.system = set([i[0:5] for i in data.system])

            logger.info('Done for %s', _sd.system)
            _sd.server.stop()
    except ConfigReadError as msg:
        logger.error(msg)
    except SSHException:
        logger.error('Could not open remote connection')
    except Exception as ex:
        exc_typ, _, exc_tb = sys.exc_info()
        logger.error('Unexpected error %s: (%s@line %s): %s',
                     ex,
                     exc_tb.tb_frame.f_code.co_filename,
                     exc_tb.tb_lineno,
                     exc_typ)
        # Stop last SSH tunnel if up
        _sd.server.stop()
    finally:
        return data, logs


def thread_wrapper(sdata, results_queue, system):
    """
    Wrapper function for main_threaded
    """
    # Get data from the remote system
    try:
        thread_sdata = sdata.clone(system)
        thread_sdata.logger.info('%s| Collecting statistics...', system)
        tunnelport = thread_sdata.server.tunnelports[system]
        if not thread_sdata.server.tunnel_is_up[tunnelport]:
            thread_sdata.logger.error('%s| System not reachable!', system)
            raise IOError
        data, log = get_system_data(thread_sdata)
        thread_sdata.logger.debug('%s| Putting results in queue', system)
    except (IOError, SFTPSessionError):
        data = pd.DataFrame()
        log = 'Could not get information from this system'
    results_queue.put((system, data, log))


def main_threaded(alldays=False, nologs=False, logger=None):
    """ Here comes the main function, with threads
    Optional: alldays (Boolean): if true, do not filter on today's date
              nologs (Boolean): if true, skip log info collection
    """
    logger = logger or init_logger()
    data = pd.DataFrame()
    logs = {}
    results_queue = queue.Queue()

    _sd = SData()
    _sd.__setattr__('logger', logger)
    _sd.__setattr__('alldays', alldays)
    _sd.__setattr__('nologs', nologs)

    # Initialize tunnel(s) and get the SSH session object
    logger.info('Initializing...')

    try:
        _sd.__setattr__('conf', read_config())
        # Initialize tunnels
        _sd.__setattr__('server', init_tunnels(_sd.conf, logger))

        if not _sd.server or not _sd.server._is_started:
            raise sshtunnel.BaseSSHTunnelForwarderError
        logger.info('Opening connection to gateway')
        _sd.server.start()

        all_systems = [x for x in _sd.conf.sections()
                       if x not in ['GATEWAY', 'MISC']]
        threads = [threading.Thread(target=thread_wrapper, name=system,
                                    args=(_sd, results_queue, system)
                                   ) for system in all_systems]
        for thread_item in threads:
            thread_item.daemon = True
            thread_item.start()

        for _ in range(len(all_systems)):
            res_sys, res_data, res_log = results_queue.get()  # 1st one to end
            logger.debug('%s| Consolidating results', res_sys)
            # concatenate with the dataframe to be returned as result
            data = pd.concat([data, res_data])
            # Group by index while keeping the metadata
            tmp_meta = copy_metadata(data)
            data = data.groupby(data.index).last()
            restore_metadata(tmp_meta, data)
            # we are only interested in first 5 chars of the system name
            data.system = set([i[0:5] for i in data.system])
            logs[res_sys] = res_log
            logger.info('%s| Done collecting data!', res_sys)

        logger.info('Stopping connection to gateway')
        _sd.server.stop()
    except sshtunnel.BaseSSHTunnelForwarderError:
        logger.error('Could not initialize the SSH tunnels, aborting')
    except ConfigReadError:
        logger.error('Could not read settings file: %s', SETTINGS_FILE)
    except AttributeError:  # raised when no server could be open
        pass
        # Stop last SSH tunnel if up
#        _sd.server.stop()
    finally:
        return data, logs

