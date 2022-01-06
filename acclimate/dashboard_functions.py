from acclimate import definitions
from acclimate import plotting


def plot_title(region_name, sector, variable, baskets=False):
    if baskets:
        return region_name + ': ' + definitions.consumption_baskets_index_name[sector] + ': ' + variable
    else:
        return region_name + ': ' + definitions.producing_sectors_index_name_dict[sector] + ': ' + variable


def get_region_timeseries(region_data, region_name, variable, sector=0, rolling_window=1):
    region_timeseries = plotting.OverlayVariableExplorer(region_data)
    region_timeseries.variable = variable
    region_timeseries.sector = sector
    region_timeseries.rolling_window = rolling_window
    viewer = region_timeseries.view
    viewer = viewer().opts(title=plot_title(region_name, sector, variable) + " timeseries")
    return viewer


def get_region_distribution(region_data, region_name, variable, sector=0, tools=True):
    region_distribution = plotting.DistributionExplorer(region_data, tools=tools)
    region_distribution.variable = variable
    region_distribution.sector = sector
    viewer = region_distribution.view
    viewer = viewer().opts(title=plot_title(region_name, sector, variable))
    return viewer


def get_region_boxplot(region_data, region_name, variable, sector=0, tools=True, baskets=False):
    region_distribution = plotting.BoxplotExplorer(region_data, groupby_variables=["agent"], tools=tools)
    region_distribution.variable = variable
    region_distribution.sector = sector
    viewer = region_distribution.view
    viewer = viewer().opts(title=plot_title(region_name, sector, variable, baskets=baskets))
    return viewer


def get_region_violin(region_data, region_name, variable, sector=0, tools=True):
    region_distribution = plotting.ViolinExplorer(region_data, tools=tools)
    region_distribution.variable = variable
    region_distribution.sector = sector
    viewer = region_distribution.view
    viewer = viewer().opts(title=plot_title(region_name, sector, variable))
    return viewer


def get_aggregated_region_timeseries(regions_ds, region_name, variable, rolling_window=1):
    region_explore = plotting.RegionDataExplorer(regions_ds)
    region_explore.variable = variable
    region_explore.region = region_name
    region_explore.rolling_window = rolling_window
    viewer = region_explore.view
    viewer = viewer().opts(title=region_name + ': aggregated statistics')
    return viewer


def get_aggregated_sector_timeseries(sector_ds, sector, variable, rolling_window=1):
    sector_explore = plotting.SectorDataExplorer(sector_ds)
    sector_name = definitions.producing_sectors_index_name_dict[sector]
    sector_explore.variable = variable
    sector_explore.sector = sector
    sector_explore.rolling_window = rolling_window
    viewer = sector_explore.view
    viewer = viewer().opts(title=sector_name + ': aggregated statistics')
    return viewer
