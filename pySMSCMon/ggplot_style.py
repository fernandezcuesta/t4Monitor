# -*- coding: utf-8 -*-
"""
Created on Wed Dec  3 15:59:03 2014

@author: fernandezjm
"""
import pylab
from matplotlib import pyplot as plt
from matplotlib.ticker import AutoMinorLocator


def rstyle(ax):
    """Styles an axes to appear like ggplot2
    Must be called after all plot and axis manipulation operations have been
    carried out (needs to know final tick spacing)
    """
    import sys
    try:
        #set the style of the major and minor grid lines, filled blocks
        ax.grid(True, 'major', color='w', linestyle='-', linewidth=1.4)
        ax.grid(True, 'minor', color='0.92', linestyle='-', linewidth=0.7)
        ax.patch.set_facecolor('0.85')
        ax.set_axisbelow(True)
       
        #set minor tick spacing to 1/2 of the major ticks
#        ax.xaxis.set_minor_locator(pylab.MultipleLocator((plt.xticks()[0][1] - \
#                                   plt.xticks()[0][0]) / 2.0))
#        ax.yaxis.set_minor_locator(pylab.MultipleLocator((plt.yticks()[0][1] - \
#                                   plt.yticks()[0][0]) / 2.0))
        ax.xaxis.set_minor_locator(pylab.MultipleLocator((ax.get_xticks()[1] - \
                                   ax.get_xticks()[0]) / 2.0))
        ax.yaxis.set_minor_locator(pylab.MultipleLocator((ax.get_xticks()[1] - \
                                   ax.get_xticks()[0]) / 2.0))
       
        #remove axis border
        for child in ax.get_children():
            if isinstance(child, pylab.matplotlib.spines.Spine):
                child.set_alpha(0)
           
        #restyle the tick lines
        for line in ax.get_xticklines() + ax.get_yticklines():
            line.set_markersize(5)
            line.set_color("gray")
            line.set_markeredgewidth(1.4)
       
        #remove the minor tick lines    
        minor = AutoMinorLocator(1)
        ax.xaxis.set_minor_locator(minor)
        ax.yaxis.set_minor_locator(minor)

#        for line in ax.xaxis.get_ticklines(minor=True) + \
#                    ax.yaxis.get_ticklines(minor=True):
#            line.set_markersize(0)
       
        #only show bottom left ticks, pointing out of axis
        pylab.rcParams['xtick.direction'] = 'out'
        pylab.rcParams['ytick.direction'] = 'out'
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
       
       
        if ax.legend_ <> None:
            lg = ax.legend_
            lg.get_frame().set_linewidth(0)
            lg.get_frame().set_alpha(0.5)
    except Exception as exc:
      print 'Exception at plot_var:', exc
      exc_typ, _, exc_tb = sys.exc_info()
      print '\rError: ({2}@line {0}): {1}'.format(exc_tb.tb_lineno,
                                                  exc_typ,
                                                  exc_tb.tb_frame.\
                                                  f_code.co_filename)

       
def rhist(ax, data, **keywords):
    """Creates a histogram with default style parameters to look like ggplot2
    Is equivalent to calling ax.hist and accepts the same keyword parameters.
    If style parameters are explicitly defined, they will not be overwritten
    """
   
    defaults = {'facecolor' : '0.3', 'edgecolor' : '0.28', 'linewidth' : '1', \
                'bins' : 100}   
    for k, v in defaults.items():
        if k not in keywords:
            keywords[k] = v
   
    return ax.hist(data, **keywords)


def rbox(ax, data, **keywords):
    """Creates a ggplot2 style boxplot, is eqivalent to calling ax.boxplot
    with the following additions:
   
    Keyword arguments:
    colors -- array-like collection of colours for box fills
    names  -- array-like collection of box names which are passed on as
              tick labels

    """

    hasColors = 'colors' in keywords
    if hasColors:
        colors = keywords['colors']
        keywords.pop('colors')
       
    if 'names' in keywords:
        ax.tickNames = plt.setp(ax, xticklabels=keywords['names'])
        keywords.pop('names')
   
    bp = ax.boxplot(data, **keywords)
    pylab.setp(bp['boxes'], color='black')
    pylab.setp(bp['whiskers'], color='black', linestyle='solid')
    pylab.setp(bp['fliers'], color='black', alpha=0.9, marker='o', \
               markersize=3)
    pylab.setp(bp['medians'], color='black')
   
    numBoxes = len(data)
    for i in range(numBoxes):
        box = bp['boxes'][i]
        boxX = []
        boxY = []
        for j in range(5):
          boxX.append(box.get_xdata()[j])
          boxY.append(box.get_ydata()[j])
        boxCoords = zip(boxX, boxY)
       
        if hasColors:
            boxPolygon = pylab.Polygon(boxCoords, \
                                       facecolor=colors[i % len(colors)])
        else:
            boxPolygon = pylab.Polygon(boxCoords, facecolor='0.95')
           
        ax.add_patch(boxPolygon)
    return bp
