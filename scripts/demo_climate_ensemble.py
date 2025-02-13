''' This script demonstrates how to load a climate ensemble from the ISIMIP bias-adjusted data and organize it into a xarray DataSet. 
In case of different grids for different models, the data can be organized into a xarray DataTree.
iparameters are:
    - basedir where data is located
    - modellist of models to be loaded.
    - scenarios to be loaded.
    - variables to be loaded.
    - ensemble_dataset: switch to generate a xarray DataSet instead of a DataTree.
'''


import glob
import os
import xarray as xr
import tqdm


basedir = '/p/projects/isimip/isimip/ISIMIP3b/InputData/climate/atmosphere/bias-adjusted/global/daily'
ensemble_dataset = True # ISMIIP bias-adjusted data is standardized to the same grid, so we can set this to True

variables = ["tas", "pr"]
scenarios = ["ssp126", "ssp370", "ssp585"]
modellist = ["ukesm1-0-ll", "gfdl-esm4", "mpi-esm1-2-hr", "mri-esm2-0", "ipsl-cm6a-lr"]  # Standard ISIMIP main models
upper_case_modellist = [model.upper() for model in modellist]

#search for all files with the variables in the basedir and model / ssp subdirectories
outputfiles = []
for variable in variables:
    for scenario in ["ssp126", "ssp370", "ssp585"]: #scenarios organised in subdirectories
        for model in upper_case_modellist: # models in subdirectories
            #get all files including the variable:
            files = (glob.glob(os.path.join(basedir,scenario,model,f'*_{variable}_*.nc')))
            outputfiles.extend(files)



#adjust search patterns based on example filename:
example_filename = 'gfdl-esm4_r1i1p1f1_w5e5_ssp126_tasmin_global_daily_2091_2100.nc'

# Categorize the files by model, ssp, and time period into a dictionary
datadict = {}

with tqdm.tqdm(outputfiles) as pbar:
    for file in outputfiles:
            #get model, ssp and time period from example filename pattern
            filename = file.split('/')[-1]
            model = str(filename.split('_')[0])
            ssp = str(filename.split('_')[3])
            timeperiod = str(filename.split('_')[7] + '_' + filename.split('_')[8].split('.')[0])
            
            #open file
            data = xr.open_dataset(file,chunks="auto")
            #add model, ssp and time period as dimensions
            data.coords['model'] = model
            data.coords['ssp'] = ssp
            #expand dimensions
            data = data.expand_dims(['model', 'ssp'])
            #wrap into dictionary by model to build data tree also for models with different grids
            if model not in datadict:
                datadict[model] = [data]
            else:
                datadict[model].append(data) 
            
#generate data tree from dictionary

#merge all data for each model, assuming data is compatible for individual models
with tqdm.tqdm(datadict) as pbar:
    for model in datadict:
        datadict[model] = xr.combine_by_coords(datadict[model])

data_tree = xr.DataTree.from_dict(datadict)

print(data_tree)

if (ensemble_dataset):
    datalist = []
    for model in datadict:
        datalist.append(datadict[model])
    dataset = xr.concat(datalist, dim='model')
    print(dataset)
