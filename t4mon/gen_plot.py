# -*- coding: utf-8 -*-
"""
Created on Mon May 25 11:10:57 2015

@author: fernandezjm
"""
from __future__ import absolute_import

import sys
from cStringIO import StringIO

import numpy as np
from matplotlib import dates as md
from matplotlib import pylab, pyplot as plt

from . import df_tools
from .logger import init_logger

DFLT_COLORMAP = 'Reds'  # default matplotlib colormap if nothing specified


def update_colors(ax, cmap=None):
    if not cmap:
        cmap = DFLT_COLORMAP
    cm = pylab.get_cmap(cmap)
    lines = ax.lines
    colors = cm(np.linspace(0, 1, len(lines)))
    for line, c in zip(lines, colors):
        line.set_color(c)


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
            plotaxis = optional.pop('ax', None)
            if not plotaxis:
                plotaxis = plt.figure().gca()
            optional['title'] = optional.pop('title', '')
            cmap = optional.pop('cmap', DFLT_COLORMAP)
            for key in optional:
                getattr(plt, key)(optional[key])
            systems = dataframe.index.get_level_values('system').unique()
            for key in systems:
                sel = df_tools.select_var(dataframe,
                                          *var_names,
                                          system=key,
                                          logger=logger)
                if sel.empty:
                    # other systems may have this column with some data
                    continue
                my_ts = [ts.to_julian_date() - 1721424.5
                         for ts in sel.index]
                for item in sel.columns:
                    logger.debug('Drawing item: %s (%s)' % (item, key))
                    # convert timestamp to number, Matplotlib requires a float
                    # format which is days since epoch
                    plt.plot(my_ts, sel[item].interpolate(),
                             label='%s %s' % (item, key))
                plt.xlim(my_ts[0], my_ts[-1])  # adjust horizontal axis
            update_colors(plotaxis, cmap)
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
