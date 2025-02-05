
"""
This script processes climate model data files by categorizing them based on model, scenario (ssp), and time period, 
and then loads the data into a hierarchical data structure using xarray's DataTree.
The script performs the following steps:
1. Determines the base directory based on the environment (local or cluster).
2. Defines glob patterns to search for specific climate model data files.
3. Searches for files matching the defined patterns.
4. Categorizes the found files into a dictionary based on model, scenario, and time period.
5. Loads the data from each file into a DataTree structure, selecting specific groups of interest.
6. Constructs a hierarchical DataTree from the categorized data.
7. Extraction of data from the Datatree into a DataSet for further processing, adding dimensions of model, ssp, timeperiod to the data.
Key variables:
- basedir: Base directory for local environment.
- clusterbasedir: Base directory for cluster environment.
- modellist: List of climate models to search for.
- ssp_globterm: Glob pattern for SSP scenarios.
- time_globterm: Glob pattern for time periods.
- group_selection: List of groups to select from the data files.
The final output is a DataTree structure containing the categorized and loaded climate model data.
"""

import glob
import os
import xarray as xr

basedir = "/home/quante/Documents/projects/post-proc-dev/"

clusterbasedir = "/p/projects/acclimate/projects/post-proc-dev/"

if os.path.exists(clusterbasedir): #TODO: better way to check if on cluster
    basedir = clusterbasedir


#adjust globterm to your needs, e.g. 
ssp_globterm = "ssp[0-9][0-9][0-9]" #looking for sspXXX scenarios
time_globterm = "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]" #looking for YYYY-YYYY time periods

modellist = ["UKESM1-0-LL", "GFDL-ESM4","MPI-ESM1-2-HR", "MRI-ESM2-0", "IPSL-CM6A-LR"] # standard isimip main models
#look for all files including one of the models
model_globterm = glob.glob(modellist[0])+glob.glob(modellist[1])+glob.glob(modellist[2])+glob.glob(modellist[3])+glob.glob(modellist[4]) #TODO: more elegant

#search for files matching the globterms

globterm = "*".join(["", time_globterm, ssp_globterm,".nc"])

outputfiles = glob.glob(basedir + globterm, recursive=True)

print(outputfiles)

#categorise the files by model, ssp and time period into a dictionary
datadict = {}
for file in outputfiles:
    model = next((model for model in modellist if model in file), None)

    ssp = "ssp"+file.split("ssp")[1][:3]

    timeperiod = "20"+file.split(model+"-20")[1][:7] #TODO: generalise

    
    if model not in datadict:
        datadict[model] = {}
    if ssp not in datadict[model]:
        datadict[model][ssp] = {}
    if timeperiod not in datadict[model][ssp]:
        datadict[model][ssp][timeperiod] = []
    datadict[model][ssp][timeperiod].append(file)
    

#load the data from each file into a data tree

#TODO: yml file for groups and variables to load?

group_selection = ["consumers"] #"regions","sectors","firms",

#variables by group
group_variables = {"regions":"ALL", "sectors":"ALL"
                   , "firms":["production_value","production_quantity","forcing"]
                   , "consumers":["consumption_value","consumption_quantity","utility"]}

#agent name converter

def data_agent_converter(data):
    if "agent" in data.dims:
        return agent_name_converter(data["agent"].values)
    else:
        print("warning: no agents found")
        return None
    


def agent_name_converter(agents):
    #convert agent from quadruple to just the first element as a string
    agent_names = []
    for agent in agents:
        agent_name = agent[0].tobytes().decode("utf-8").rstrip('\x00')
        if agent_name:
            agent_names.append(agent_name)
    #get regions from AGRI agents
    regions = [agent.split(":")[1] for agent in agent_names if 'AGRI' in agent]
    #change quintiles to shorthand
    quintiles = ["first_income_quintile","second_income_quintile","third_income_quintile","fourth_income_quintile","fifth_income_quintile"]
    new_quintiles = ["q1","q2","q3","q4","q5"]
    
    new_consumer_names = []
    old_consumer_names = []
    region = "AFG"
    for agent_name in agent_names:
        if 'AGRI:' in agent_name:
            region = agent_name.split(":")[1]
        for quintile, new_quintile in zip(quintiles,new_quintiles):
            if quintile in agent_name:
                old_consumer_names.append(agent_name)
                new_agent_name = new_quintile+":"+region
                new_consumer_names.append(new_agent_name)

    #replace old consumer names with new names:
    new_agent_names = agent_names.copy()
    for old, new in zip(old_consumer_names,new_consumer_names):
        new_agent_names = [new if agent is old else agent for agent in new_agent_names]
    
    print(new_agent_names)
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
                            print("renaming agents")
                            output_dict[group]["agent"] = data_agent_converter(raw_output[group].to_dataset())
                            print(output_dict[group]["agent"])
                        #select group variables and remove dims not in remaining_dims
                        if group_variables[group] != "ALL":
                            remaining_dims = raw_output[group].to_dataset(inherit=False)[group_variables[group]].dims
                        else:
                            remaining_dims = raw_output[group].to_dataset(inherit=False).dims
                        for dim in output_dict[group].dims:
                            if dim not in remaining_dims:
                                output_dict[group] = output_dict[group].drop_vars(dim)
                        print(output_dict[group])
                       
            timeperiod_dict[timeperiod] = xr.DataTree.from_dict(output_dict)
        ssp_dict[ssp] = xr.DataTree.from_dict(timeperiod_dict)
    model_dict[model] = xr.DataTree.from_dict(ssp_dict)
data_tree = xr.DataTree.from_dict(model_dict)
            
print(data_tree)

#extract data from the data tree into a dataset, selecting specific groups from the DataTree

#TODO: more robust way to extract model, ssp, timeperiod
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
                #convert model, ssp, timeperiod to coordinates and dimensions
                data = data.set_coords(["model", "ssp", "timeperiod"])
                data = data.expand_dims(["model", "ssp", "timeperiod"])
                mergelist.append(data)
    final_dataset = xr.merge(mergelist)
    print(final_dataset)
    to_compute.append(final_dataset.to_netcdf(basedir + "output_" + group + ".nc",compute=False))

import dask
from dask.diagnostics import ProgressBar

with ProgressBar():
    dask.compute(to_compute)
