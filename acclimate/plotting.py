# TODO plotting functions, i.a. for maps, interactve widgets for notebooks
import math

import holoviews as hv
import param
from holoviews.operation.timeseries import rolling

from acclimate import definitions

golden = (1 + math.sqrt(5)) / 2


class StorageVariableExplorer(param.Parameterized):

    def __init__(self, dataset, groupby_variables=["agent"], tools=True):
        super().__init__()
        self.dataset = dataset
        self.groupby_variables = groupby_variables
        self.tools = tools
        variable_names = [i_dim.name for i_dim in self.dataset.vdims]
        self.variable = param.ObjectSelector(default=variable_names[0], objects=variable_names)
        self.sector = param.ObjectSelector(default=0, objects=definitions.producing_sectors_name_index_dict)
        self.rolling_window = param.Integer(default=1, bounds=(1, 1000))
        self.agent = param.ObjectSelector(default=0,
                                          objects=range(0, len(definitions.producing_sectors_name_index_dict)))

    @param.depends('rolling_window', 'variable', 'sector', 'agent')
    def load_symbol(self):
        selection = self.dataset.select(sector=self.sector, agent=self.agent)
        curve = hv.Curve(selection, 'time', self.variable)
        curve.opts(color=definitions.agent_colors[self.agent])
        return curve

    @param.depends('rolling_window', 'variable', 'sector', 'agent')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        return (dmap).opts(aspect=golden)


class OverlayVariableExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        selection = self.dataset.select(sector=self.sector)
        curve = selection.to(hv.Curve, "time", self.variable, groupby=self.groupby_variables).overlay()
        curve.opts(hv.opts.Curve(color=hv.Cycle(definitions.agent_colors), show_legend=False))
        return curve

    @param.depends('rolling_window', 'variable', 'sector', 'agent')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        # add rolling mean
        smoothed = rolling(dmap, rolling_window=self.rolling_window)
        # TODO: add outliers
        # outliers = rolling_outlier_std(dmap, rolling_window=self.rolling_window).opts(
        #   color='red', marker='triangle', framewise=True)
        return (smoothed).opts(hv.opts.Curve(aspect=golden))


class DistributionExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        curve = data.to(hv.Distribution, self.variable, [], groupby=self.groupby_variables).overlay()
        curve.opts(hv.opts.Distribution(filled=True, color=hv.Cycle(definitions.agent_colors), alpha=0.9))
        if not self.tools:
            curve.opts(hv.opts.NdOverlay(toolbar=None, responsive=False, active_tools=[], default_tools=[]))
        return curve


class BoxplotExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        boxplot = data.to(hv.BoxWhisker, self.groupby_variables, self.variable, groupby="sector").overlay()
        boxplot.opts(hv.opts.BoxWhisker(box_fill_color='agent', cmap=definitions.agent_colors_list))
        if not self.tools:
            boxplot.opts(hv.opts.NdOverlay(toolbar=None, responsive=False, active_tools=[], default_tools=[]))
        return boxplot


class ViolinExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        plot = data.to(hv.Violin, self.groupby_variables, self.variable, groupby="sector").overlay()
        plot.opts(hv.opts.Violin(violin_fill_color='agent', cmap=definitions.agent_colors_list))
        if not self.tools:
            plot.opts(hv.opts.NdOverlay(toolbar=None, responsive=False, active_tools=[], default_tools=[]))
        return plot


class RegionDataExplorer(StorageVariableExplorer):

    def __init__(self, dataset):
        super().__init__(dataset)
        self.dataset.kdims[0].value_format = definitions.region_map
        self.region = param.ObjectSelector(default=0, objects=definitions.regions_name_index_dict)

    @param.depends('rolling_window', 'variable', 'region')
    def load_symbol(self):
        selection = self.dataset.select(region=self.region)
        curve = selection.to(hv.Curve, 'time', self.variable).overlay()
        curve.opts(hv.opts.Curve(show_legend=False))
        return curve

    # wrapping super().view() due to different param dependencies
    @param.depends('rolling_window', 'variable', 'region')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        smoothed = rolling(dmap, rolling_window=self.rolling_window)
        # TODO: add outliers
        # outliers = rolling_outlier_std(dmap, rolling_window=self.rolling_window).opts(
        #   color='red', marker='triangle', framewise=True)
        return (smoothed).opts(aspect=golden)


class SectorDataExplorer(StorageVariableExplorer):

    def __init__(self, dataset):
        super().__init__(dataset)
        self.dataset.kdims[0].value_format = definitions.producing_sector_map
        self.sector = param.ObjectSelector(default=0, objects=definitions.producing_sectors_name_index_dict)

    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        selection = self.dataset.select(sector=self.sector)
        curve = selection.to(hv.Curve, 'time', self.variable).overlay()
        curve.opts(hv.opts.Curve(show_legend=False))
        return curve

    # wrapping super().view() due to different param dependencies
    @param.depends('rolling_window', 'variable', 'region')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        smoothed = rolling(dmap, rolling_window=self.rolling_window)
        # TODO: add outliers
        # outliers = rolling_outlier_std(dmap, rolling_window=self.rolling_window).opts(
        #   color='red', marker='triangle', framewise=True)
        return (smoothed).opts(aspect=golden)


class AgentVariableExplorer(param.Parameterized):

    def __init__(self, dataset, groupby_variables=["agent"]):
        super().__init__()
        self.dataset = dataset
        variable_names = [i_dim.name for i_dim in self.dataset.vdims]
        self.variable = param.ObjectSelector(default=variable_names[0], objects=variable_names)
        self.rolling_window = param.Integer(default=1, bounds=(1, 1000))
        self.agent = param.ObjectSelector(default=0,
                                          objects=range(0, len(self.dataset.agent)))
        self.groupby_variables = groupby_variables

    @param.depends('rolling_window', 'variable', 'agent')
    def load_symbol(self):
        selection = self.dataset.select(agent=self.agent)
        curve = hv.Curve(selection, 'time', self.variable)
        curve.opts(color=definitions.agent_colors[self.agent])
        return curve

    @param.depends('rolling_window', 'variable', 'agent')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        return (dmap).opts(aspect=golden)


class AgentOverlayVariableExplorer(AgentVariableExplorer):
    @param.depends('rolling_window', 'variable')
    def load_symbol(self):
        selection = self.dataset
        curve = selection.to(hv.Curve, 'time', self.variable, groupby=self.groupby_variables).overlay()
        curve.opts(hv.opts.Curve(color=hv.Cycle(definitions.agent_colors), show_legend=False))
        return curve

    @param.depends('rolling_window', 'variable', 'agent')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        # add rolling mean
        smoothed = rolling(dmap, rolling_window=self.rolling_window)
        # TODO: add outliers
        # outliers = rolling_outlier_std(dmap, rolling_window=self.rolling_window).opts(
        #   color='red', marker='triangle', framewise=True)
        return (smoothed).opts(aspect=golden)


class AgentDistributionExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable')
    def load_symbol(self):
        data = self.dataset
        curve = data.to(hv.Distribution, self.variable, [], groupby=self.groupby_variables).overlay()
        curve.opts(hv.opts.Distribution(filled=True, color=hv.Cycle(definitions.agent_colors), alpha=0.9))
        return curve


class AgentBoxplotExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable')
    def load_symbol(self):
        data = self.dataset
        boxplot = data.to(hv.BoxWhisker, 'agent', self.variable, groupby=self.groupby_variables).overlay()
        boxplot.opts(hv.opts.BoxWhisker(box_fill_color='agent', cmap=definitions.agent_colors_list))
        return boxplot


class AgentViolinExplorer(StorageVariableExplorer):
    @param.depends('rolling_window', 'variable')
    def load_symbol(self):
        data = self.dataset
        plot = data.to(hv.Violin, 'agent', self.variable, groupby=self.groupby_variables).overlay()
        plot.opts(hv.opts.Violin(violin_fill_color='agent', cmap=definitions.agent_colors_list))
        return plot


import itertools
from matplotlib.cbook import _reshape_2D
import numpy as np


# https://stackoverflow.com/questions/54911424/giving-custom-inter-quartile-range-for-boxplot-in-matplotlib
# Function adapted from matplotlib.cbook
def my_boxplot_stats(X, whis=1.5, bootstrap=None, labels=None,
                     autorange=False, percents=[25, 75]):
    def _bootstrap_median(data, N=5000):
        # determine 95% confidence intervals of the median
        M = len(data)
        percentiles = [2.5, 97.5]

        bs_index = np.random.randint(M, size=(N, M))
        bsData = data[bs_index]
        estimate = np.median(bsData, axis=1, overwrite_input=True)

        CI = np.percentile(estimate, percentiles)
        return CI

    def _compute_conf_interval(data, med, iqr, bootstrap):
        if bootstrap is not None:
            # Do a bootstrap estimate of notch locations.
            # get conf. intervals around median
            CI = _bootstrap_median(data, N=bootstrap)
            notch_min = CI[0]
            notch_max = CI[1]
        else:

            N = len(data)
            notch_min = med - 1.57 * iqr / np.sqrt(N)
            notch_max = med + 1.57 * iqr / np.sqrt(N)

        return notch_min, notch_max

    # output is a list of dicts
    bxpstats = []

    # convert X to a list of lists
    X = _reshape_2D(X, "X")

    ncols = len(X)
    if labels is None:
        labels = itertools.repeat(None)
    elif len(labels) != ncols:
        raise ValueError("Dimensions of labels and X must be compatible")

    input_whis = whis
    for ii, (x, label) in enumerate(zip(X, labels)):

        # empty dict
        stats = {}
        if label is not None:
            stats['label'] = label

        # restore whis to the input values in case it got changed in the loop
        whis = input_whis

        # note tricksyness, append up here and then mutate below
        bxpstats.append(stats)

        # if empty, bail
        if len(x) == 0:
            stats['fliers'] = np.array([])
            stats['mean'] = np.nan
            stats['med'] = np.nan
            stats['q1'] = np.nan
            stats['q3'] = np.nan
            stats['cilo'] = np.nan
            stats['cihi'] = np.nan
            stats['whislo'] = np.nan
            stats['whishi'] = np.nan
            stats['med'] = np.nan
            continue

        # up-convert to an array, just to be safe
        x = np.asarray(x)

        # arithmetic mean
        stats['mean'] = np.mean(x)

        # median
        med = np.percentile(x, 50)
        ## Altered line
        q1, q3 = np.percentile(x, (percents[0], percents[1]))

        # interquartile range
        stats['iqr'] = q3 - q1
        if stats['iqr'] == 0 and autorange:
            whis = 'range'

        # conf. interval around median
        stats['cilo'], stats['cihi'] = _compute_conf_interval(
            x, med, stats['iqr'], bootstrap
        )

        # lowest/highest non-outliers
        if np.isscalar(whis):
            if np.isreal(whis):
                loval = q1 - whis * stats['iqr']
                hival = q3 + whis * stats['iqr']
            elif whis in ['range', 'limit', 'limits', 'min/max']:
                loval = np.min(x)
                hival = np.max(x)
            else:
                raise ValueError('whis must be a float, valid string, or list '
                                 'of percentiles')
        else:
            loval = np.percentile(x, whis[0])
            hival = np.percentile(x, whis[1])

        # get high extreme
        wiskhi = np.compress(x <= hival, x)
        if len(wiskhi) == 0 or np.max(wiskhi) < q3:
            stats['whishi'] = q3
        else:
            stats['whishi'] = np.max(wiskhi)

        # get low extreme
        wisklo = np.compress(x >= loval, x)
        if len(wisklo) == 0 or np.min(wisklo) > q1:
            stats['whislo'] = q1
        else:
            stats['whislo'] = np.min(wisklo)

        # compute a single array of outliers
        stats['fliers'] = np.hstack([
            np.compress(x < stats['whislo'], x),
            np.compress(x > stats['whishi'], x)
        ])

        # add in the remaining stats
        stats['q1'], stats['med'], stats['q3'] = q1, med, q3

    return bxpstats
