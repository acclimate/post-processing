#!/usr/bin/env python3
import argparse
import os

import dask
import xarray as xr
from dask_jobqueue import SLURMCluster

from acclimate import dataset, definitions
from acclimate import helpers


# some functions TODO: put into source file after development

def regionalize_data(regions, data, outputdir, output, sectors=None, agent_type="consumer",
                     filenamestub="baseline_relative_consumer_storage_"):
    results = []
    for i_region in regions:
        filtered_data = single_region_data(i_region, data, output, sectors=sectors, agent_type=agent_type)
        baseline_data = baseline_relative(filtered_data)
        results.append(safe_file(baseline_data, filenamestub + i_region + ".nc", outputdir))
    return results


@dask.delayed
def single_region_data(region, data, output, sectors=None, agent_type="consumer"):
    # filter for groups of regions
    if region in definitions.WORLD_REGIONS.keys():
        return aggregate_subregions(region, data, output, sectors=sectors, agent_type=agent_type)
    else:
        return select_storage(region, data, output, sectors=sectors, agent_type=agent_type)


def aggregate_subregions(region_group, data, output, sectors=None, agent_type="consumer"):
    regions_to_aggregate = list(set(definitions.WORLD_REGIONS[region_group]) - set(["BLR", "MDA", "SDN", "ZWE"]))
    aggregated_data = []
    for i_subregion in regions_to_aggregate:
        if i_subregion in definitions.WORLD_REGIONS.keys():
            subdata = aggregate_subregions(i_subregion, data, output,
                                           sectors=sectors,
                                           agent_type=agent_type)  # recursion to get aggregate of aggregate regions
        else:
            subdata = select_storage(i_subregion, data, output, sectors=sectors, agent_type=agent_type)
        aggregated_data.append(subdata)
    aggregate = xr.concat(aggregated_data, "region")
    return aggregate.sum("region", skipna=True)


def select_storage(region, data, output, sectors=None, agent_type="consumer"):
    return helpers.select_partial_data(
        helpers.select_by_agent_properties(data, output, type=agent_type, region=region),
        sector=sectors)


@dask.delayed
def baseline_relative(data):
    return data.map(dataset.baseline_relative)


@dask.delayed
def safe_file(data, filename, outputdir):
    data.to_netcdf(os.path.join(outputdir, filename))
    return (os.path.join(outputdir, filename))


parser = argparse.ArgumentParser(description="Process acclimate output for easy and fast plotting.")
parser.add_argument(
    "--acclimate_output"
    , type=str,
    required=True,
    help="Path to the acclimate data file (default: CURRENT/output.nc)"
)
parser.add_argument(
    "--outputdir"
    , type=str,
    required=True,
    help="Output directory (default: CURRENT)"
)
parser.add_argument(
    "--regions"
    , type=str,
    nargs='+',
    required=True,
    help="Regions to analyse"
)

args = parser.parse_args()
if not args.acclimate_output:
    args.acclimate_output = os.path.join(os.getcwd(), 'output.nc')
if not args.outputdir:
    args.outputdir = os.getcwd()

# experimental use of adaptive dask cluster to speed up operations if capacity is available on the cluster
# create dask cluster utilitzing dask.jobqueue
cluster = SLURMCluster(
    job_extra=["--qos standby"],
    queue="priority",
    project="acclimat",
    cores=4,
    memory="16 GiB",
    walltime="0-00:10:00",
    extra=["--lifetime", "10", "--lifetime-stagger", "1m"]
)
print(cluster.job_script())  # ask for max 10 jobs

output = dataset.AcclimateOutput(args.acclimate_output)

if args.regions[0] == "CORE":
    region_groups = ["USA", "CHN", "EU28", "G20_REST", "BRIS"]
    regions_seperate = list((definitions.WORLD_REGIONS["USA"], definitions.WORLD_REGIONS["CHN"],
                             definitions.WORLD_REGIONS["EU28"], definitions.WORLD_REGIONS["G20"], region_groups))
    regions = list(set([i_region for i_list in regions_seperate for i_region in i_list]))
    regions.sort()
    args.regions = regions

if args.regions[0] == "ALL":
    regions_seperate = list(definitions.WORLD_REGIONS.values())
    regions_seperate.append(definitions.WORLD_REGIONS.keys())  # append by aggregation regions
    regions = list(set([i_region for i_list in regions_seperate for i_region in i_list]) - set(
        ["BLR", "MDA", "SDN", "ZWE"]))
    regions.sort()
    args.regions = regions

print(args.regions)

storage_data = output.xarrays["storages"]

# experimental use of dask delayed to parallelize
# aggregated storage data to check consumption patterns
aggregated_storage_data = dask.delayed(helpers.aggregate_by_sector_group(storage_data, definitions.consumption_baskets))
# delay data as advised in https://docs.dask.org/en/latest/delayed-best-practices.html
storage_data = dask.delayed(storage_data)
output = dask.delayed(output)

cluster.adapt(minimum=16, maximum=512)  # ask for max 512 cores as limit of standby queue

print("Crunching numbers for single regions...")
dask.compute(regionalize_data(args.regions, storage_data, args.outputdir, output,
                              sectors=list(definitions.producing_sectors_name_index_dict.values())))
dask.compute(regionalize_data(args.regions, aggregated_storage_data, args.outputdir, output, sectors=[0, 1, 2],
                              filenamestub="baseline_relative_consumer_basket_storage_"))

# get storage data for other sectors
dask.compute(regionalize_data(args.regions, storage_data, args.outputdir, output,
                              sectors=list(definitions.producing_sectors_name_index_dict.values()),
                              agent_type="firm", filenamestub="baseline_relative_firm_storage_"))

# calculate consumer data
consumer_data = output.xarrays["consumers"]
dask.compute(regionalize_data(args.regions, consumer_data, args.outputdir, output,
                              sectors=None, filenamestub="baseline_relative_consumer_data_"))

# calculate firm data
firm_data = output.xarrays["firms"]
dask.compute(regionalize_data(args.regions, firm_data, args.outputdir, output,
                              sectors=None, filenamestub="baseline_relative_firm_data_"))

# get baseline relative region data
print("aggregated region statistics are calculated...")
region_data = output.xarrays["regions"].compute()
baseline_region_data = region_data.map(dataset.baseline_relative)
baseline_region_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))
print("Done.")
