# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:10:57 2015

@author: fernandezjm
"""
from __future__ import absolute_import

import sys
from cStringIO import StringIO

from matplotlib import dates as md
from matplotlib import pyplot as plt

from . import df_tools
from .logger import init_logger


def plot_var(dataframe, *var_names, **optional):
    """
    Plots the specified variable names from the dataframe overlaying
    all plots for each variable and silently skipping unexisting variables.

    - Optionally selects which system to filter on (i.e. system='localhost')
    - Optionally sends keyword parameters to pyplot (**optional)

    var_names: Filter column names that match any var_names; each individual
               var_item in var_names (first one if we also filter on system)
               can have wildcards ('*') like 'str1*str2'; in that case the
               column name must contain both 'str1' and 'str2'.
    """
    logger = optional.pop('logger', '') or init_logger()
    try:
        system_filter = optional.pop('system', '')
        if dataframe.empty:
            raise TypeError

        # If we filter by system: only first column in var_names will be
        # selected, dataframe.plot() function will be used.
        if system_filter:
            sel = df_tools.select_var(dataframe,
                                      *var_names,
                                      system=system_filter,
                                      logger=logger)
            if sel.empty:
                raise TypeError
            # Linear interpolation for missing values
            plotaxis = sel.interpolate().plot(**optional)

        # Otherwise, var_names columns are selected for system in the dataframe
        # and matplotlib.pyplot's plot function is used once for each column.
        else:
            plt.set_cmap(optional.pop('cmap',
                                      optional.pop('colormap', 'Reds')))
            optional['title'] = optional.pop('title', var_names[0].upper())
            plotaxis = plt.figure().gca()
            for key in optional:
                getattr(plt, key)(optional[key])
            # TODO: is there a way to directly plot a multiindex DF?
            for key in dataframe.index.get_level_values('system').unique():
                sel = df_tools.select_var(dataframe,
                                          *var_names,
                                          system=key,
                                          logger=logger)
                if sel.empty:
                    # other systems may have this column with some data
                    continue
                for item in sel.columns:
                    logger.debug('Drawing item: %s (%s)' % (item, key))
                    # convert timestamp to number, Matplotlib requires a float
                    # format which is days since epoch
                    my_ts = [ts.to_julian_date() - 1721424.5
                             for ts in sel.dropna().index]
                    plt.plot(my_ts,
                             sel.dropna(), label='%s@%s' % (item, key))
        # Style the resulting plot
        plotaxis.xaxis.set_major_formatter(md.DateFormatter('%d/%m/%y\n%H:%M'))
        plotaxis.legend(loc='best')
        return plotaxis
    except TypeError:
        logger.error('%s%s not drawn%s',
                     '{} | '.format(system_filter) if system_filter else '',
                     var_names,
                     ' for this system' if system_filter else '')
    except Exception as exc:
        item, item, exc_tb = sys.exc_info()
        logger.error('Exception at plot_var (line %s): %s',
                     exc_tb.tb_lineno,
                     repr(exc))
    item = plt.figure()
    return item.gca()


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
        encoded_plot = 'data:image/png;base64,%s' %\
                       fbuffer.getvalue().encode("base64")
        fbuffer.close()
        return encoded_plot
    except AttributeError:
        return ''
