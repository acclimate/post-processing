"""
This script processes ensemble data for storage sensitivity analysis, calculates metrics, and saves results.

The script demonstrates the following steps:
1. Defines directories and identifiers.
2. Finds filenames with parameters encoded in the filename.
3. Loads and processes files.
4. Checks for completeness of the time dimension and tidies agents in the dataset.
5. Combines the files by coordinates.
6. Saves the combined data to a NetCDF file.
7. Loads data for analysis.
8. Calculates metrics such as medians and aggregates for each ensemble member.
9. Saves results to NetCDF files.
10. Loads NetCDF files and saves results as CSV for plotting.
"""

import os
import re
import sys

import dask
import tqdm
import xarray as xr
from dask.diagnostics import ProgressBar

import postproc_acclimate.helpers as helpers
import postproc_acclimate.definitions as defs
import postproc_acclimate.ensemble_data_combination as edc
import postproc_acclimate.analysis_functions as analysis
import postproc_acclimate.data_transform as datatransform

# Define directories and identifiers
identifier = "ensemble_storage_forcing_amplitude"
basedir = "/p/projects/acclimate/projects/storage-sensitivity"
ensembledir = os.path.join(basedir, "runs")
analysisdir = os.path.join(basedir, "analysis")

if not os.path.exists(analysisdir):
    os.makedirs(analysisdir)

# Find filenames with parameters encoded in the filename
# Example filename: "storage_capacity_6_forcing_amplitude_1.5_DOSE_TAS_PR_MPI-ESM1-2-HR-ssp370_2024-2034.nc"
pattern = re.compile(
    r"storage_capacity_(?P<storage_capacity>\d+)_" +
    r"forcing_amplitude_(?P<forcing_amplitude>\d+\.\d+)_" +
    r"DOSE_TAS_PR_(?P<model>[a-zA-Z0-9\-]+)-" +
    r"(?P<scenario>ssp[0-9\-]+)_" +
    r"(?P<timeperiod>\d+-\d+)\.nc"
)

group_variables = {
    "firms": ["production_value", "production_quantity", "forcing"],
}

with tqdm.tqdm(total=1, desc="Loading and processing files", leave=True, file=sys.stdout) as pbar:
    ensemble_data_list = edc.load_ensemble_files(ensembledir, pattern, "firms", group_variables["firms"])
    pbar.update(1)

# Check for each file in the list: if the time dimension is complete, add it to the list of files to merge,
# and tidy the agents in the dataset for easier processing
files_to_merge = []
with tqdm.tqdm(total=len(ensemble_data_list), desc="Tidying agents", leave=True, file=sys.stdout) as pbar:
    for dataset in ensemble_data_list:
        if len(dataset.time) == 4020:
            files_to_merge.append(helpers.tidy_agents(dataset))
        pbar.update(1)

# Combine the files by coords
with tqdm.tqdm(total=1, desc="Combining files by coords", leave=True, file=sys.stdout) as pbar:
    ensemble_data = xr.combine_by_coords(files_to_merge)
    pbar.update(1)

print(ensemble_data, flush=True)

# Save to netcdf before analysis
ensemble_data.to_netcdf(os.path.join(ensembledir, f"acclimate_output_{identifier}_firms.nc"), compute=False)
with ProgressBar(dt=10):
    dask.compute(ensemble_data)

# Load data for analysis
with tqdm.tqdm(total=1, desc="Loading data for analysis", leave=True, file=sys.stdout) as pbar:
    firm_ensemble_data = xr.open_dataset(
        os.path.join(analysisdir, f"acclimate_output_{identifier}_firms.nc"),
        chunks='auto'
    )
    pbar.update(1)
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

with tqdm.tqdm(total=1, desc="Transforming data to region, sector format", leave=True, file=sys.stdout) as pbar:
    data = datatransform.add_region_sector(firm_ensemble_data)  # convert to region, sector format
    pbar.update(1)

with tqdm.tqdm(total=1, desc="Calculating medians", leave=True, file=sys.stdout) as pbar:
    results["medians"] = data.quantile([0.5], dim="time")
    pbar.update(1)

with tqdm.tqdm(total=1, desc="Calculating baseline and aggregates", leave=True, file=sys.stdout) as pbar:
    results["baseline_aggregates_regions"], results["aggregates_regions"] = analysis.get_baseline_and_aggregates(
        data, baseline_date, "region", aggregate_region_dict
    )
    pbar.update(1)

# Save results to NetCDF files
with tqdm.tqdm(total=1, desc="Preparing saving to NetCDF files", leave=True, file=sys.stdout) as pbar:
    to_save = [
        results[calc_key].to_netcdf(os.path.join(analysisdir, f"{identifier}_firms_{calc_key}.nc"), compute=False)
        for calc_key in results.keys()
    ]
    pbar.update(1)

with ProgressBar(dt=10):
    dask.compute(*to_save, num_workers=os.cpu_count())

# Load NetCDF files and save as CSV for plotting
with tqdm.tqdm(total=3, desc="Loading results from NetCDF files", leave=True, file=sys.stdout) as pbar:
    medians = xr.open_dataset(os.path.join(analysisdir, f"{identifier}_firms_medians.nc"))
    pbar.update(1)
# Load netcdf files
with tqdm.tqdm(total=3, desc="Loading results from NetCDF files", leave=True, file=sys.stdout) as pbar:
    aggregates = xr.open_dataset(os.path.join(analysisdir, f"{identifier}_firms_aggregates_regions.nc"), chunks='auto')
    pbar.update(1)
    baseline_aggregates = xr.open_dataset(os.path.join(analysisdir, f"{identifier}_firms_baseline_aggregates_regions.nc"), chunks='auto')
    aggregates = xr.open_dataset(os.path.join(analysisdir, f"{identifier}_firms_aggregates_regions.nc"),chunks='auto')

# Save to CSV
with tqdm.tqdm(total=1, desc="Saving medians to csv", leave=True, file=sys.stdout) as pbar:
    medians.to_dataframe().to_csv(os.path.join(analysisdir, f"{identifier}_firms_medians.csv"))
    pbar.update(1)
with tqdm.tqdm(total=1, desc="Calculating baseline deviation", leave=True, file=sys.stdout) as pbar:
    baseline_deviation = (aggregates - baseline_aggregates) / baseline_aggregates
    with ProgressBar(dt=10):
        dask.compute(baseline_deviation)
with tqdm.tqdm(total=1, desc="Saving baseline deviation to csv", leave=True, file=sys.stdout) as pbar:
    baseline_deviation.to_dataframe().to_csv(os.path.join(analysisdir, f"{identifier}_firms_baseline_deviation.csv"))
    pbar.update(1)