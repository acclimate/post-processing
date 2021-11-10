# TODO plotting functions, i.a. for maps, interactve widgets for notebooks
import math

import holoviews as hv
import param
from holoviews.operation.timeseries import rolling

from acclimate import definitions

golden = (1 + math.sqrt(5)) / 2


class VariableExplorer(param.Parameterized):

    def __init__(self, dataset):
        super().__init__()
        self.dataset = dataset
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


class OverlayVariableExplorer(VariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        selection = self.dataset.select(sector=self.sector)
        curve = selection.to(hv.Curve, 'time', self.variable, groupby='agent').overlay()
        curve.opts(hv.opts.Curve(color=hv.Cycle(definitions.agent_colors)))
        return curve

    @param.depends('rolling_window', 'variable', 'sector', 'agent')
    def view(self):
        dmap = hv.DynamicMap(self.load_symbol)
        # add rolling mean
        smoothed = rolling(dmap, rolling_window=self.rolling_window)
        # TODO: add outliers
        # outliers = rolling_outlier_std(dmap, rolling_window=self.rolling_window).opts(
        #   color='red', marker='triangle', framewise=True)
        return (smoothed).opts(aspect=golden)


class DistributionExplorer(VariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        curve = data.to(hv.Distribution, self.variable, [], groupby='agent').overlay()
        curve.opts(hv.opts.Distribution(filled=True, color=hv.Cycle(definitions.agent_colors), alpha=0.9))
        return curve


class BoxplotExplorer(VariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        boxplot = data.to(hv.BoxWhisker, 'agent', self.variable, groupby="sector").overlay()
        boxplot.opts(hv.opts.BoxWhisker(box_fill_color='agent', cmap=definitions.agent_colors_list))
        return boxplot


class ViolinExplorer(VariableExplorer):
    @param.depends('rolling_window', 'variable', 'sector')
    def load_symbol(self):
        data = self.dataset.select(sector=self.sector)
        plot = data.to(hv.Violin, 'agent', self.variable, groupby="sector").overlay()
        plot.opts(hv.opts.Violin(violin_fill_color='agent', cmap=definitions.agent_colors_list))
        return plot


class RegionDataExplorer(VariableExplorer):

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
