# TODO helper functions for dealing with acclimate output

import netCDF4


def agent_index(variable, sector=None, region=None, agent_type=None):
    """
    Get indices of agents for the dimensions in `variable`.

    Parameters
    ----------
    variable
        Variable for which the indices are to be used
    sector
        None or int or str or iterable of str : Sector(s) of agents to select
    region
        None or int or str or iterable of str : Region(s) of agents to select
    agent_type
        None or int or str or iterable of str : Type(s) of agents to select

    Returns
    -------
    List[int]
        List of indices for the selected agents
    """

    if sector and agent_type is None:
        agent_type = "consumer"

    if sector and agent_type == "consumer":
        raise ValueError("Cannot give sector when selecing consumer agents")

    def remap(v, lookupdict):
        if v is None:
            return lookupdict.values()
        elif isinstance(v, int):
            return [v]
        elif isinstance(v, str):
            return remap(lookupdict[v], lookupdict)
        else:
            return [k for i in v for k in remap(i, lookupdict)]

    if isinstance(variable, netCDF4.Variable):
        # TODO way to cache agents, sectors, region
        nc = variable.group()
        agents = nc["agent"]
        sector = remap(sector, nc["sector"])
        region = remap(region, nc["region"])
        agent_type = remap(agent_type, nc["agent_type"])
    else:
        sector = remap(sector, variable.sectors)
        region = remap(region, variable.regions)
        agent_type = remap(agent_type, variable.agent_types)

    return [
        i
        for i, v in enumerate(agents)
        if v[1] in agent_type and v[2] in sector and v[3] in region
    ]
