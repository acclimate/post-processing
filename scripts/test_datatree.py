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
    
print(datadict)

#load the data from each file into a data tree
#select certain groups
group_selection = ["firms","consumers","regions","sectors"]

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
                        output_dict[group] = raw_output[group]
            timeperiod_dict[timeperiod] = xr.DataTree.from_dict(output_dict)
        ssp_dict[ssp] = xr.DataTree.from_dict(timeperiod_dict)
    model_dict[model] = xr.DataTree.from_dict(ssp_dict)
data_tree = xr.DataTree.from_dict(model_dict)
            
print(data_tree)
