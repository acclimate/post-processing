# TODO helper functions for dealing with acclimate output datasets
import os

import holoviews as hv
import xarray as xr

from acclimate import definitions, analysis


def select_partial_data(data, sector=None, region=None, agent=None):
    """
      Get part of dataset selected by the parameters.

      Parameters
      ----------
      data
          xarray dataset to select on
      sector
          None or int or str or iterable of int : Sector(s) of data to select
      region
          None or int or iterable of int : Region(s) of data to select
      agent
          None or int or iterable of int : Indizes of agents for which data to select

      Returns
      -------
      xarray.Dataset
          dataset with the selected subset of input data
      """
    if sector is not None:
        data = data.sel(sector=sector)
    if region is not None:
        data = data.sel(region=region)
    if agent is not None:
        data = data.sel(agent=agent)
    return data


def select_by_agent_properties(data, acclimate_output, sector=None, region=None, type=None):
    """
      Get part of dataset selected by the agents fitting the parameters.
      Parameters
      ----------
      data
          xarray dataset to select on
      acclimate_output
        AcclimateOutput for agent properties
      sector
          None or int or str or iterable of int : Sector(s) of agents to select
      region
          None or int or iterable of int : Region(s) of agents to select
      type
          None or int or iterable of int : Type of agents to select

      Returns
      -------
      xarray.Dataset
          dataset with the selected subset of data for the agents with given properties
      """
    agents = acclimate_output.agent(type=type, region=region, sector=sector)
    return data.sel(agent=agents)


def aggregate_by_sector_group(data, sector_groups):
    """
    Aggregates data by given sector groups
      Parameters
      ----------
      data
        xarray data to be aggregated on the sector dimension
      sector_groups
        dictionary with sector group names as keys and sector names as values

      Returns
      -------
      xarray.Dataset
        aggregated data with sector dimension reduced to the given sector groups
    """
    aggregated_data = []
    for i_group in sector_groups.keys():
        sector_indizes = [definitions.producing_sectors_name_index_dict[i_sector] for i_sector in
                          sector_groups[i_group]]
        aggregate = data.sel(sector=sector_indizes).sum("sector", skipna=True)
        aggregated_data.append(aggregate)
    return xr.concat(aggregated_data, "sector")


# some helpers on data exploration

def clean_vdims_consumption(data, agent_map=definitions.consumer_map, agent_label="income_qunitile",
                            sector_map=definitions.producing_sector_map):
    for dimension in data.kdims:
        if dimension.name == "agent":
            dimension.value_format = agent_map
            dimension.label = agent_label
        if dimension.name == "sector":
            dimension.value_format = sector_map
            dimension.values = list(range(0, 26))
        if dimension.name == "time":
            dimension.label = "Time"
    return data


def load_region_data(datadir, filename, region, sector_map=definitions.producing_sector_map, elasticities=False,
                     agent_map=definitions.consumer_map, agent_label="income_quintile"):
    data = xr.open_dataset(os.path.join(datadir, filename + region + ".nc"))
    dataset = hv.Dataset(data)
    if elasticities:
        dataset = analysis.calculate_empiricial_elasticities(dataset)
        dataset = analysis.rolling_window_elasticity(dataset)
        dataset = analysis.savgol_elasticity(dataset)

    return clean_vdims_consumption(dataset, sector_map=sector_map, agent_map=agent_map, agent_label=agent_label)


def load_region_basket_data(datadir, filename, region, elasticities=True):
    return load_region_data(datadir, filename, region, sector_map=definitions.basket_map, elasticities=elasticities)
