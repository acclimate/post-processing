'''
Helper functions for dealing with acclimate output datasets.
'''

from toolbox_acclimate import definitions
import warnings

def data_agent_converter(data):
    """
    Convert agent data to a more readable format.

    Parameters
    ----------
    data : xarray.Dataset
        The dataset containing agent data.

    Returns
    -------
    list
        Converted agent names.
    """
    def agent_name_converter(agents):
        """
        Convert agent names from quadruple to a string format.

        Parameters
        ----------
        agents : list
            List of agent quadruples.

        Returns
        -------
        list
            List of converted agent names.
        """
        agent_names = []
        for agent in agents:
            agent_name = agent[0].tobytes().decode("utf-8").rstrip('\x00')
            if agent_name:
                agent_names.append(agent_name)
                
        quintiles = definitions.long_quintiles
        new_quintiles = definitions.short_quintiles

        new_consumer_names = []
        old_consumer_names = []
        region = agent_names[0].split(":")[1]
        for agent_name in agent_names:
            if 'income_quintile' not in agent_name:
                region = agent_name.split(":")[1]
            for quintile, new_quintile in zip(quintiles, new_quintiles):
                if quintile in agent_name:
                    old_consumer_names.append(agent_name)
                    new_agent_name = new_quintile + ":" + region
                    new_consumer_names.append(new_agent_name)

        new_agent_names = agent_names.copy()
        for old, new in zip(old_consumer_names, new_consumer_names):
            new_agent_names = [new if agent is old else agent for agent in new_agent_names]
        return new_agent_names

    if "agent" in data.dims:
        return agent_name_converter(data["agent"].values)
    else:
        warnings.warn("No agents found in the dataset", UserWarning)
        return None

def tidy_agents(dataset, group_to_load="firms"):
    """
    Tidy up agent names in the dataset and optionally filter by group.

    Parameters
    ----------
    dataset : xarray.Dataset
        The dataset containing agent data.
    group_to_load : str, optional
        The group of agents to load, by default "firms".

    Returns
    -------
    xarray.Dataset
        The dataset with tidied agent names.
    """
    if "agent" in dataset.dims:
        new_agent_names = data_agent_converter(dataset['agent'])
        dataset = dataset.assign_coords(agent=new_agent_names)
        
        regions = [agent.split(":")[1] for agent in new_agent_names]
        unique_regions = []
        for region in regions:
            if region not in unique_regions:
                unique_regions.append(region)
        consumer_agents = [quintile+":"+region for quintile in definitions.short_quintiles for region in unique_regions]
        other_agents = [agent for agent in new_agent_names if agent not in consumer_agents]
        if group_to_load == "consumers":
            dataset = dataset.sel(agent=consumer_agents)
        else:
            dataset = dataset.sel(agent=other_agents)
    return dataset