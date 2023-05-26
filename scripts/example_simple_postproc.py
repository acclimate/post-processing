#!/usr/bin/env python
# coding: utf-8

import glob
import os
import dask

from acclimate import dataset

basedir = #TODO: path to acclimate outputs
globterm = #TODO: search term to identify outputs (if all nc files, just use "/**/*.nc"
outputfiles = glob.glob(basedir + globterm, recursive=True)

variables = #TODO: acclimate variables to load
group_to_load = "firms"
agent_type = "firm"

def save_select_variables(datapath, variables,group_to_load,agent_type, datadir):
    output = dataset.AcclimateOutput(datapath, groups_to_load=[group_to_load]).sel(agent_type=agent_type)
    data = output[variables].data
    simulationident = datapath.split("/")[-1].split(".nc")[0]
    #TODO: improve laziness of this procedure for parallelization
    return data.compute().to_netcdf(os.path.join(datadir, agent_type +"_"+ simulationident + ".nc"),compute=False)

datadir = #TODO: path to safe the data


from dask_jobqueue import SLURMCluster
from dask.distributed import Client

project = "acclimat"
cluster = SLURMCluster(
    job_extra_directives=["--qos priority", "partition=priority"],
    cores=3,
    memory="12GiB",
    project=project,
    walltime="00-00:60:00",
    log_directory="dask-worker-space/logs",
    local_directory="tmp",
    interface="ib0"
    #,worker_extra_args=["--lifetime", "25m", "--lifetime-stagger", "4m"]
)
client = Client(cluster)

cluster.scale(n=4)
# wait for some workers
client.wait_for_workers(4)

precompute = []
for output in outputfiles:
    try:
        precompute.append(save_select_variables(output, variables,group_to_load,agent_type, datadir))
    except:
        print(output)

client.compute(precompute)

# this generates netcdf files for handling with xarray, e.g. aggregate by sector / region and compare between runs

