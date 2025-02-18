import glob
import os
import xarray as xr
import dask

import postproc_acclimate.definitions as defs
import postproc_acclimate.data_transform as datatransform
import postproc_acclimate.analysis_functions as analysis
import postproc_acclimate.ensemble_data_combination as edc
import postproc_acclimate.helpers as helpers

# Define base directories and identifiers
identifier = "test_ensemble_202502"
basedir = "/p/projects/acclimate/projects/post-proc-dev/"

group_selection = ["firms"]
group_variables = {
    "firms": ["production_value", "production_quantity", "forcing"],
}

# Create ensemble data tree
data_tree = edc.create_ensemble_datatree(
    basedir, group_selection, group_variables, data_agent_converter=helpers.data_agent_converter
)

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

# Save datasets to NetCDF files
to_compute = [
    dataset_dict[group].to_netcdf(f"{basedir}acclimate_output_{identifier}_{group}.nc", compute=False)
    for group in group_selection
]

dask.compute(to_compute)

# Load data for analysis
firm_ensemble_data = xr.open_mfdataset(
    glob.glob(os.path.join(basedir, f"acclimate_output_{identifier}_firms.nc")),
    combine="by_coords",
    chunks='auto'
)
print(firm_ensemble_data, flush=True)

# Calculate some metrics for each ensemble member
results = {
    "medians": {},
    "aggregates_regions": {},
    "baseline_aggregates_regions": {}
}

aggregate_region_dict = {
    "USA": defs.WORLD_REGIONS["USA"],
    "CHN": defs.WORLD_REGIONS["CHN"],
    "EU27": defs.WORLD_REGIONS["EU27"]
}
baseline_date = firm_ensemble_data.time.values[0]

data = datatransform.add_region_sector(firm_ensemble_data)
results["medians"] = data.quantile([0.5], dim="time")
results["baseline_aggregates_regions"], results["aggregates_regions"] = analysis.get_baseline_and_aggregates(
    data, baseline_date, "region", aggregate_region_dict
)

# Save results to NetCDF files
to_save = [
    results[calc_key].to_netcdf(os.path.join(basedir, f"{identifier}_firms_{calc_key}.nc"), compute=False)
    for calc_key in results.keys()
]

dask.compute(to_save)

# Transform the NetCDF data to CSV for plotting
medians = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_medians.nc"))
aggregates_regions = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_aggregates_regions.nc"))
baseline_aggregates_regions = xr.open_dataset(os.path.join(basedir, f"{identifier}_firms_baseline_aggregates_regions.nc"))

# Aggregate medians across sectors
medians_aggregated = medians.sum(dim="sector")
medians_aggregated.to_dataframe().to_csv(os.path.join(basedir, f"{identifier}_firms_medians_aggregated.csv"))

# Aggregate aggregates_regions across sectors
aggregates_regions_aggregated = aggregates_regions.sum(dim="sector")
baseline_aggregates_regions_aggregated = baseline_aggregates_regions.sum(dim="sector")

# Calculate baseline relative values, and save timeseries for each region_aggregated
baseline_relative_deviation = (aggregates_regions_aggregated - baseline_aggregates_regions_aggregated) / baseline_aggregates_regions_aggregated
baseline_relative_deviation.to_dataframe().to_csv(os.path.join(basedir, f"{identifier}_firms_baseline_relative_deviation.csv"))