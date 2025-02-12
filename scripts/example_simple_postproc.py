import glob
import os
import xarray as xr
import dask


import postproc_acclimate.definitions as defs
import postproc_acclimate.data_transform as datatransform
import postproc_acclimate.analysis_functions as analysis
import postproc_acclimate.ensemble_data_combination as edc

import postproc_acclimate.helpers as helpers

basedir = "/p/projects/acclimate/projects/post-proc-dev/"

identifier = "test_ensemble_202502"

groups = ["firms"]


basedir = "/home/quante/Documents/projects/post-proc-dev/"
identifier = "test_ensemble_202502"
clusterbasedir = "/p/projects/acclimate/projects/post-proc-dev/"

if os.path.exists(clusterbasedir):
    basedir = clusterbasedir

group_selection = ["firms"]
group_variables = {
    "regions": "ALL",
    "sectors": "ALL",
    "firms": ["production_value", "production_quantity", "forcing"],
    "consumers": ["consumption_value", "consumption_quantity", "utility"]
}

data_tree = edc.create_ensemble_datatree(basedir, group_selection, group_variables,data_agent_converter=helpers.data_agent_converter)

models = list(data_tree.keys())
ssps = set()
timeperiods = set()

for model in models:
    ssps.update(data_tree[model].keys())
    for ssp in data_tree[model].keys():
        timeperiods.update(data_tree[model][ssp].keys())

ssps = list(ssps)
timeperiods = list(timeperiods)

dataset_dict = edc.datatree_to_dataset_dict(data_tree, group_selection)

to_compute = [dataset_dict[group].to_netcdf(f"{basedir}acclimate_output_{identifier}_{group}.nc", compute=False) for group in group_selection]

dask.compute(to_compute)

#load data for analysis
firm_ensemble_data = xr.open_mfdataset(glob.glob(os.path.join(basedir,"acclimate_output_"+identifier+"_firms.nc")),combine="by_coords",chunks='auto')
print(firm_ensemble_data,flush=True)

#calculate some metrics for each ensemble member
results = {}
results["medians"] = {}
results["aggregates_regions"] = {}
results["baseline_aggregates_regions"] = {}


keys = []
aggregate_region_dict = {"USA": defs.WORLD_REGIONS["USA"], "CHN": defs.WORLD_REGIONS["CHN"], "EU27": defs.WORLD_REGIONS["EU27"]}
baseline_date = firm_ensemble_data.time.values[0]

ensembledims = ["model", "scenario", "timeperiod"]

# for model in models:
#     for ssp in ssps:
#         for timeperiod in timeperiods:
#             data = firm_ensemble_data.sel({ensembledims[0]:model,ensembledims[1]:ssp,ensembledims[2]:timeperiod}, drop=False)
#             data = datatransform.add_region_sector(data)  # Transform from agent to region and sector dimensions
            
#             # Calculate median
#             results["medians"][(model, ssp, timeperiod)] = data.quantile([0.5], dim="time")
#             keys.append((model, ssp, timeperiod))
            
#             # Calculate baseline and aggregate data for regions
#             baseline_and_aggregates = analysis.get_baseline_and_aggregates(data, baseline_date, "region", aggregate_region_dict)
#             results["baseline_aggregates_regions"][(model, ssp, timeperiod)] = baseline_and_aggregates[0]
#             results["aggregates_regions"][(model, ssp, timeperiod)] = baseline_and_aggregates[1]
            
# to_save = [
#     xr.combine_by_coords([results[calc_key][key] for key in keys])
#     .to_netcdf(os.path.join(basedir, f"{identifier}_firms_{calc_key}.nc"), compute=False)
#     for calc_key in results.keys()]


#TODO: alternative calculate directy for all ensemble members
data = datatransform.add_region_sector(firm_ensemble_data)
baseline_date = firm_ensemble_data.time.values[0]
results["medians"] = data.quantile([0.5], dim="time")
results["baseline_aggregates_regions"], results["aggregates_regions"] = analysis.get_baseline_and_aggregates(data, baseline_date, "region", aggregate_region_dict)

to_save = [results[identifier].to_netcdf(os.path.join(basedir, f"{identifier}_firms_{calc_key}.nc"), compute=False) for calc_key in results.keys()]


dask.compute(to_save)


#transform the netcdf data to csv for plotting

#load data
medians = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_medians.nc"))
aggregates_regions = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_aggregates_regions.nc"))
baseline_aggregates_regions = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_baseline_aggregates_regions.nc"))

#aggregate medians across sectors
medians_aggregated = medians.sum(dim="sector")
medians_aggregated.to_dataframe().to_csv(os.path.join(basedir, f"{identifier}_firms_medians_aggregated.csv"))

#aggregate aggregates_regions across sectors
aggregates_regions_aggregated = aggregates_regions.sum(dim="sector")
baseline_aggregates_regions_aggregated = baseline_aggregates_regions.sum(dim="sector")

#calculate baseline relative values, and save timeseries for each region_aggregated
baseline_relative_deviation = (aggregates_regions_aggregated - baseline_aggregates_regions_aggregated) / baseline_aggregates_regions_aggregated
baseline_relative_deviation.to_dataframe().to_csv(os.path.join(basedir, f"{identifier}_firms_baseline_relative_deviation.csv"))