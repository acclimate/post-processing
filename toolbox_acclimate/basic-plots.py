# TODO less specific, maybe more generic plots. 
# TODO: most used "normal" acclimate plots, i.e. timeseries, boxplots, etc. 

import numpy as np
import matplotlib.pyplot as plt


#generic plotting routine from an xarray dataset:
# 1. select towards 2 remaining main dimensions / variables
#   a) (e.g. time and region)
#   b) (e.g. category + different summary metrics (mean, median, etc. -> boxplot like))
#   c) scatterplot comparing values for different entities (e.g. regions)
# 2. plot a (sub)set of variables into the plot
# this script provides examples for basic plots, assuming the selection of the data is already done (as shown in data-selection.py)


#some definitions for plotting  #TODO: load color lists from yml file
from matplotlib.colors import LinearSegmentedColormap

def pik_color(tone, id=0):
    return {
        "orange": ["#fab792", "#f89667", "#f57744", "#f35a28", "#de5224", "#c94a20", "#b4421c"],
        "gray": ["#bec1c3", "#a0a4a7", "#83888b", "#686c70", "#55585b", "#434346", "#302f32"],
        "blue": ["#99dff9", "#66cef6", "#33bef3", "#00adef", "#008dc7", "#006e9e", "#004e75"],
        "green": ["#cce3b7", "#b3d698", "#9aca7c", "#81be63", "#6a9c51", "#537a3f", "#3d582d"]
    }[tone][3 - id]


pik_cols = {'blue': pik_color('blue', 0),
            'orange': pik_color('orange', 0),
            'green': pik_color('green', -1),
            'gray': pik_color('gray', -1)}

pik_colors = map(lambda i: pik_color(
    ['blue', 'green', 'orange', 'gray'][int(i % 4)], [0, 3, -3, 1, -1, 2, -2][int(i / 4)]), range(20))


def pik_color_list(n_of_elements, col_list=[pik_color('green'), pik_color('blue'), pik_color('orange')]):
    c_map = LinearSegmentedColormap.from_list('my_colors', col_list, N=n_of_elements)
    return [(c_map(1. * i / (n_of_elements - 1))) for i in np.arange(0, n_of_elements)]


plot_colors = pik_color_list(5)
import matplotlib.colors

plot_colors_list = [matplotlib.colors.to_hex(i_color) for i_color in plot_colors] 


#basic plotting types

#timeseries plot of a variable,
# potentially with confidence intervals

def plot_timeseries(data, variable, color = plot_colors[0], lower_confidence_interval = None, upper_confidence_interval = None, title=None, ylabel=None, xlabel=None, figsize=(12, 6)):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    data[variable].plot(ax=ax,color=color)
    if lower_confidence_interval is not None:
        ax.fill_between(data.time.data, lower_confidence_interval, upper_confidence_interval, color=color, alpha=0.5)
    if title is not None:
        ax.set_title(title)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    return fig, ax

#scatterplot of two variables for different entities

def plot_scatter(data, x_variable, y_variable, size_variable,label_variable, legend=True, title=None, ylabel=None, xlabel=None, figsize=(12, 6)):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    ax.scatter(data[x_variable], data[y_variable], s=data[size_variable], c=data[label_variable], cmap= pik_color_list(len(data[label_variable].unique())))
    if legend is True:
        ax.legend()
    if title is not None:
        ax.set_title(title)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    return fig, ax
    
    
#boxplot of a variable from a timeseries

def plot_boxplot(data, timeseries, quartiles = [0.25,0.75], whiskers=[5,95], title=None, ylabel=None, xlabel=None, figsize=(12, 6)):
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    ax.boxplot(data[timeseries].values, q=quartiles, whis=whiskers, showfliers=False)
    
    if title is not None:
        ax.set_title(title)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    return fig, ax
    



