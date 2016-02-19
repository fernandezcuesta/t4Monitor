# -*- coding: utf-8 -*-
"""
**Plot generation helper methods**
"""
from __future__ import absolute_import

import sys
import base64

import six
import numpy as np
from matplotlib import dates as md
from matplotlib import pyplot as plt
from matplotlib import pylab

from . import df_tools
from .logger import init_logger

DFLT_COLORMAP = 'cool'  # default matplotlib colormap if nothing specified

# Initialize default figure sizes and styling
pylab.rcParams['figure.figsize'] = 13, 10
plt.style.use('ggplot')


__all__ = ('plot_var', 'to_base64',)


def update_colors(ax, cmap=None):
    """
    Update colormap for a plot given its axis
    """
    if not cmap:
        cmap = DFLT_COLORMAP
    cm = pylab.get_cmap(cmap)
    lines = ax.lines
    colors = cm(np.linspace(0, 1, len(lines)))
    for line, c in zip(lines, colors):
        line.set_color(c)


def plot_var(dataframe, *args, **kwargs):
    """
    Plot the specified variable names from the dataframe overlaying
    all plots for each variable and silently skipping non-existing variables.

    Inherently remove outliers out from +/- 3 standard deviations away
    from mean.

    Arguments:
        dataframe (pandas.DataFrame): Source of the data to be plotted
        \*args (str):
            Filter column names that match any item in ``\*args``; each
            item can represent a valid regular expression.
    Keyword Arguments:
        system (Optional[str]):
            select which system to filter on (i.e. ``system='localhost'``)
        **kwargs (Optional):
            Keyword parameters passed transparently to pyplot

    Return:
        matplotlib.figure.Figure
    """
    logger = kwargs.pop('logger', '') or init_logger()

    try:
        system_filter = kwargs.pop('system', '')
        assert not dataframe.empty
        # If we filter by system: only first column in var_names will be
        # selected, dataframe.plot() function will be used.
        if system_filter:
            sel = df_tools.select(dataframe,
                                  *args,
                                  system=system_filter,
                                  logger=logger)
            if sel.empty:
                raise TypeError
            # Remove outliers (>3 std away from mean)
            sel = df_tools.remove_outliers(sel.dropna(), n_std=3)
            plotaxis = sel.plot(**kwargs)
            update_colors(plotaxis, kwargs.get('cmap', DFLT_COLORMAP))
        else:
            plotaxis = plot_var_by_system(dataframe, *args, **kwargs)

        # Style the resulting plot axis and legend
        plotaxis.xaxis.set_major_formatter(md.DateFormatter('%d/%m/%y\n%H:%M'))
        plotaxis.legend(loc='best')
        return plotaxis
    except (TypeError, AssertionError):
        logger.error('{0}{1} not drawn{2}'.format(
                     '{0} | '.format(system_filter) if system_filter else '',
                     args,
                     ' for this system' if system_filter else ''))
    except Exception as exc:
        item, item, exc_tb = sys.exc_info()
        logger.error('Exception at plot_var (line {0}): {1}'
                     .format(exc_tb.tb_lineno, repr(exc)))
    # Return an empty figure if an exception was raised
    item = plt.figure()
    return item.gca()


def plot_var_by_system(dataframe, *var_names, **optional):
    """
    Replace pandas' ``DataFrame.plot()`` to allow plotting different systems in
    the same axis.

    ``var_names`` columns are selected for system in the dataframe
    and ``matplotlib.pyplot``'s plot function is used once for each column.
    """
    logger = optional.pop('logger', '') or init_logger()
    plotaxis = optional.pop('ax', None) or plt.figure().gca()
    cmap = optional.pop('cmap', DFLT_COLORMAP)
    systems = dataframe.index.get_level_values('system').unique()
    for system in systems:
        sel = df_tools.select(dataframe,
                              *var_names,
                              system=system,
                              logger=logger)
        if sel.empty:  # other systems may have this column with some data
            continue
        # # Remove outliers (>3 std away from mean)
        # sel = df_tools.remove_outliers(sel.dropna(), n_std=3)
        for item in sel.columns:
            logger.debug('Drawing item: {0} ({1})'.format(item, system))
            plotaxis = sel[item].plot(label='{0} {1}'.format(item, system),
                                      **optional)
    update_colors(plotaxis, cmap)
    return plotaxis


def to_base64(dataframe_plot, img_fmt=None):
    """Convert a plot into base64-encoded graph (PNG by default)

    Arguments:
        ataframe_plot (AxesSubplot):
            Figure obtained from drawing a dataframe object

        img_fmt (Optional[str]):
            Format of the resulting image. This format is tightly coupled to
            the backend used by matplotlib. Defaults to 'png'.
    Return:
        str
    """
    if not img_fmt:
        img_fmt = 'png'

    try:  # check there's data and the backend supports the output format
        assert (dataframe_plot.has_data() and img_fmt in
                dataframe_plot.get_figure().canvas.get_supported_filetypes())
    except AssertionError:
        return ''

    fbuffer = six.BytesIO()
    fig = dataframe_plot.get_figure()
    fig.savefig(fbuffer,
                format=img_fmt,
                bbox_inches='tight')
    fbuffer.seek(0)
    encoded_plot = six.b('data:image/{0};base64,'
                         .format(img_fmt)) + base64.b64encode(fbuffer.read())
    fbuffer.close()
    return encoded_plot
