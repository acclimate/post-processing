"""
This script processes ensemble model output data files by categorizing them based on model, scenario (ssp), and time period,
and then loads the data into a hierarchical data structure using xarray's DataTree.

The script performs the following steps:
1. Determines the base directory based on the environment (local or cluster).
2. Defines glob patterns to search for specific model output files.
3. Searches for files matching the defined patterns.
4. Categorizes the found files into a dictionary based on model, scenario, and time period.
5. Loads the data from each file into a DataTree structure, selecting specific groups of interest.
6. Constructs a hierarchical DataTree from the categorized data.
7. Extraction of data from the Datatree into a DataSet for further processing, adding dimensions of model, ssp, timeperiod to the data.

Key variables:
- basedir: Base directory for local environment.
- identifier: Identifier for the output files.
- clusterbasedir: Base directory for cluster environment.
- modellist: List of (climate) models to search for.
- ssp_globterm: Glob pattern for SSP scenarios.
- time_globterm: Glob pattern for time periods.
- group_selection: List of groups to select from the data files.

The final output are ensemble datasets for each selected group, containing data from all models, scenarios, and time periods.
"""

import glob
import os
import xarray as xr
import tqdm
import dask
from dask.diagnostics import ProgressBar

basedir = "/home/quante/Documents/projects/post-proc-dev/"
identifier = "test_ensemble_202502"
clusterbasedir = "/p/projects/acclimate/projects/post-proc-dev/"

if os.path.exists(clusterbasedir):  # TODO: better way to check if on cluster
    basedir = clusterbasedir

ssp_globterm = "ssp[0-9][0-9][0-9]"  # looking for sspXXX scenarios
time_globterm = "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]"  # looking for YYYY-YYYY time periods

modellist = ["UKESM1-0-LL", "GFDL-ESM4", "MPI-ESM1-2-HR", "MRI-ESM2-0", "IPSL-CM6A-LR"]  # standard isimip main models
model_globterm = sum([glob.glob(model) for model in modellist], [])  # TODO: more elegant

globterm = "*".join(["", time_globterm, ssp_globterm, ".nc"])
outputfiles = glob.glob(basedir + '**' + globterm, recursive=True)

print(outputfiles, flush=True)

datadict = {}
for file in outputfiles:
    model = next((model for model in modellist if model in file), None)
    ssp = "ssp" + file.split("ssp")[1][:3]
    timeperiod = "20" + file.split(model + "-20")[1][:7]  # TODO: generalise

    if model not in datadict:
        datadict[model] = {}
    if ssp not in datadict[model]:
        datadict[model][ssp] = {}
    if timeperiod not in datadict[model][ssp]:
        datadict[model][ssp][timeperiod] = []
    datadict[model][ssp][timeperiod].append(file)

group_selection = ["firms"]  # "regions", "sectors", "consumers"
group_variables = {
    "regions": "ALL",
    "sectors": "ALL",
    "firms": "ALL",
    "consumers": ["consumption_value", "consumption_quantity", "utility"]
}

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
    if "agent" in data.dims:
        return agent_name_converter(data["agent"].values)
    else:
        print("warning: no agents found")
        return None

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

    regions = [agent.split(":")[1] for agent in agent_names if 'AGRI' in agent]
    quintiles = ["first_income_quintile", "second_income_quintile", "third_income_quintile", "fourth_income_quintile", "fifth_income_quintile"]
    new_quintiles = ["q1", "q2", "q3", "q4", "q5"]

    new_consumer_names = []
    old_consumer_names = []
    region = "AFG"
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

model_dict = {}
for model, modeldict in datadict.items():
    ssp_dict = {}
    for ssp, sspdict in modeldict.items():
        timeperiod_dict = {}
        for timeperiod, files in sspdict.items():
            for file in files:
                if len(files) > 1:
                    print("Warning: multiple files found for model", model, "ssp", ssp, "timeperiod", timeperiod)
                else:
                    raw_output = xr.open_datatree(files[0])
                    output_dict = {}
                    for group in group_selection:
                        output_dict[group] = raw_output[group].to_dataset()
                        if "agent" in raw_output[group].dims:
                            output_dict[group]["agent"] = data_agent_converter(raw_output[group].to_dataset())
                            output_dict[group] = output_dict[group].reindex(agent=output_dict[group]["agent"])

                        if group_variables[group] != "ALL":
                            remaining_dims = raw_output[group].to_dataset(inherit=False)[group_variables[group]].dims
                        else:
                            remaining_dims = raw_output[group].to_dataset(inherit=False).dims
                        for dim in output_dict[group].dims:
                            if dim not in remaining_dims:
                                output_dict[group] = output_dict[group].drop_vars(dim)

            timeperiod_dict[timeperiod] = xr.DataTree.from_dict(output_dict)
        ssp_dict[ssp] = xr.DataTree.from_dict(timeperiod_dict)
    model_dict[model] = xr.DataTree.from_dict(ssp_dict)
data_tree = xr.DataTree.from_dict(model_dict)

print(data_tree, flush=True)

models = list(data_tree.keys())
ssps = list(data_tree[models[0]].keys())
timeperiods = list(data_tree[models[0]][ssps[0]].keys())

to_compute = []
for group in group_selection:
    mergelist = []
    for model in models:
        for ssp in ssps:
            for timeperiod in timeperiods:
                data = data_tree[model][ssp][timeperiod][group]
                data = data.to_dataset(inherit=False)
                data["model"] = model
                data["ssp"] = ssp
                data["timeperiod"] = timeperiod
                data = data.set_coords(["model", "ssp", "timeperiod"])
                data = data.expand_dims(["model", "ssp", "timeperiod"])
                mergelist.append(data)
    final_dataset = xr.merge(mergelist)
    print(final_dataset, flush=True)

    if group == "consumers" or group == "firms":
        quintiles = ["q1", "q2", "q3", "q4", "q5"]
        agents = final_dataset["agent"].values.tolist()
        consumer_agents = [agent for agent in agents if any(quintile in agent for quintile in quintiles)]
        if group == "consumers":
            final_dataset = final_dataset.sel(agent=consumer_agents)
        else:
            firm_agents = [agent for agent in agents if agent not in consumer_agents]
            final_dataset = final_dataset.sel(agent=firm_agents)

    to_compute.append(final_dataset.to_netcdf(basedir + "acclimate_output_" + identifier + "_" + group + ".nc", compute=False))

with ProgressBar(flush=True):
    dask.compute(to_compute)
