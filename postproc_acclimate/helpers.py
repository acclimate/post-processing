# TODO helper functions for dealing with acclimate output datasets
# TODO: adjust to new overarching structure based on xarray?!
from postproc_acclimate import definitions
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