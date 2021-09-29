# TODO helper functions for dealing with acclimate output datasets

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
