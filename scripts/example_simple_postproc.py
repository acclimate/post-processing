import glob
import os
import xarray as xr
import dask

import postproc_acclimate.definitions as defs
import postproc_acclimate.data_transform as datatransform
import postproc_acclimate.analysis_functions as analysis

basedir = "/p/projects/acclimate/projects/post-proc-dev/"

identifier = "test_ensemble_202502"

groups = ["firms"]

#get nc files with identifier
files = {}
for group in groups:
    files[group] = glob.glob(basedir + "/*output*"+identifier+"*"+group+"*.nc", recursive=False)
print(files)

ensembledimensions = ["model","ssp","timeperiod"]
#get model, ssp, and timeperiods from the datafiles (=ensemble dimensions)
firm_ensemble_data = xr.open_mfdataset(files["firms"], chunks = 'auto')

models = [str(model) for model in firm_ensemble_data.model.values.tolist()]
ssps = [str(ssp) for ssp in firm_ensemble_data.ssp.values.tolist()]
timeperiods = [str(timeperiod) for timeperiod in firm_ensemble_data.timeperiod.values.tolist()]


print(models)
print(ssps)
print(timeperiods)

#get remaining dimensions =data dimensions
data_dimensions = list(firm_ensemble_data.dims)
for dim in ensembledimensions:
    data_dimensions.remove(dim)
print(data_dimensions)


#split data along the ensemble dimensions

#number of realisations
ensemblemembers = len(firm_ensemble_data["model"])*len(firm_ensemble_data["ssp"])*len(firm_ensemble_data["timeperiod"])
print(ensemblemembers)

#TODO: think about create dask.cluster with one worker for each ensemble member, up to a limit of 64 workers

#apply function to each ensemble member using dask

#calculate quantiles for each ensemble member
results = {}
results["medians"] = {}
results["baselines"] = {}
results["aggregates_regions"] = {}

calculations = ["quantiles","baselines","aggregates_regions"]
keys = []
for model in models:
    for ssp in ssps:
        for timeperiod in timeperiods:
            data = firm_ensemble_data.sel(model=model,ssp=ssp,timeperiod=timeperiod,drop=False) #drop = False to keep scalar dimensions for re-combining
            #transform from agent to region and sector dimensions
            data = datatransform.add_region_sector(data)
            print(data,flush=True)
            #assumption_baseline data = first day of time dimension
            baseline_date = data.time.values[0]
            results["baselines"][(model,ssp,timeperiod)] = datatransform.get_baseline_data(data,baseline_date)
            results["medians"][(model,ssp,timeperiod)] = data.quantile([0.5],dim="time")
            keys.append((model,ssp,timeperiod))
            
            #calculate aggregate data for usa chn eu27 using definitions.WORLD_REGIONS
            aggregate_region_dict = {"USA":defs.WORLD_REGIONS["USA"],"CHN":defs.WORLD_REGIONS["CHN"],"EU27":defs.WORLD_REGIONS["EU27"]}
            results["aggregates_regions"][(model,ssp,timeperiod)] = analysis.aggregate_by_dimension_dict(data,"region",aggregate_region_dict)
                
            

to_save = []
#combine each calculation into one dataset, using the ensemble dimensions as coordinates
for calc_key in calculations:
    result = xr.combine_by_coords([results[calc_key][key].compute() for key in keys])
    #save to netcdf
    combination_identifier = identifier+ "_firms_"+calc_key
    to_save.append(result.to_netcdf(os.path.join(basedir,combination_identifier+".nc"),compute=False))
#compute and save
dask.compute(to_save)