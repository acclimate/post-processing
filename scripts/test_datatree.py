"""
Processes ensemble model output files by categorizing them based on model, scenario (ssp), and time period,
and loads the data into a hierarchical structure using xarray's DataTree.

Steps:
1. Set base directory based on environment.
2. Define glob patterns for model output files.
3. Search and categorize files.
4. Load data into a DataTree structure.
5. Extract data into a DataSet for further processing.

Key variables:
- basedir: Base directory for local environment.
- identifier: Identifier for the output files.
- clusterbasedir: Base directory for cluster environment.
- modellist: List of models to search for.
- group_selection: List of groups to select from the data files.

Final output: Ensemble datasets for each selected group.
"""

import os
import dask
from dask.diagnostics import ProgressBar
import postproc_acclimate.ensemble_data_combination as edc
import postproc_acclimate.definitions as definitions

basedir = "/home/quante/Documents/projects/post-proc-dev/"
identifier = "test_ensemble_202502"
clusterbasedir = "/p/projects/acclimate/projects/post-proc-dev/"

if os.path.exists(clusterbasedir):
    basedir = clusterbasedir

datadict = edc.find_ensemble_files(basedir, modellist=["UKESM1-0-LL", "GFDL-ESM4", "MPI-ESM1-2-HR", "MRI-ESM2-0", "IPSL-CM6A-LR"])

group_selection = ["firms"]
group_variables = {
    "regions": "ALL",
    "sectors": "ALL",
    "firms": "ALL",
    "consumers": ["consumption_value", "consumption_quantity", "utility"]
}

data_tree = edc.process_datadict(datadict, group_selection, group_variables, data_agent_converter=data_agent_converter)

models = list(data_tree.keys())
ssps = set()
timeperiods = set()

for model in models:
    ssps.update(data_tree[model].keys())
    for ssp in data_tree[model].keys():
        timeperiods.update(data_tree[model][ssp].keys())

ssps = list(ssps)
timeperiods = list(timeperiods)

dataset = edc.datatree_to_dataset(data_tree, group_selection)

to_compute = []
for group in group_selection:
    dataset = dataset[group]
    if group in ["consumers", "firms"]:
        agents = dataset["agent"].values.tolist()
        consumer_agents = [agent for agent in agents if any(quintile in agent for quintile in definitions.short_quintiles)]
        if group == "consumers":
            dataset = dataset.sel(agent=consumer_agents)
        else:
            firm_agents = [agent for agent in agents if agent not in consumer_agents]
            dataset = dataset.sel(agent=firm_agents)
    to_compute.append(dataset.to_netcdf(f"{basedir}acclimate_output_{identifier}_{group}.nc", compute=False))

with ProgressBar(flush=True):
    dask.compute(to_compute)
