#!/usr/bin/env python3
import argparse
import os

import dask
import xarray as xr
from dask_jobqueue import SLURMCluster

from acclimate import dataset, definitions
from acclimate import helpers

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


# some functions TODO: put into source file after development

def regionalize_data(regions, data, output, sectors=None, agent_type="consumer",
                     filenamestub="baseline_relative_consumer_storage_", aggregate_regions=True,
                     data_baseline_relative=False):
    files = {}
    for i_region in regions:
        filtered_data = single_region_data(i_region, data, output, sectors=sectors, agent_type=agent_type,
                                           aggregate_regions=aggregate_regions)
        # first filter, than baseline relative to reduce memory load
        baseline_data = filtered_data
        if not baseline_relative:
            baseline_data = baseline_relative(filtered_data)
        filename = filenamestub + i_region + ".nc"
        files[filename] = baseline_data
    return files


def single_region_data(region, data, output, sectors=None, agent_type="consumer", aggregate_regions=True):
    # filter for groups of regions
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        if region in definitions.WORLD_REGIONS.keys():
            if aggregate_regions:
                return aggregate_subregions(region, data, output, sectors=sectors, agent_type=agent_type)
            else:
                return select_storage(region, data, output, sectors=sectors,
                                      agent_type=agent_type)  # TODO: proper labelling of output to be able to filter
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

def baseline_relative(data):
    return data.map(dataset.baseline_relative)

def safe_file(data, filename, outputdir):
    return data.to_netcdf(os.path.join(outputdir, filename), compute=False)


def safe_file_dict(filedict, outputdir):
    files = []
    for i_filename in filedict.keys():
        files.append(safe_file(filedict[i_filename], i_filename, outputdir))
    return files



# experimental use of adaptive dask cluster to speed up operations if capacity is available on the cluster
# create dask cluster utilitzing dask.jobqueue
cluster = SLURMCluster(
    job_extra=["--qos standby"],
    queue="standard",
    project="acclimat",
    cores=4,
    memory="16 GB",
    walltime="0-00:10:00",
    extra=["--lifetime", "10", "--lifetime-stagger", "1m"]
)

cluster.adapt(minimum=0, maximum=64)
# cluster.scale(n=32)
from dask.distributed import Client

client = Client(cluster)

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
    regions = list(set([i_region for i_list in regions_seperate for i_region in i_list]) - {"BLR", "MDA", "SDN", "ZWE"})
    regions.sort()
    args.regions = regions

print(args.regions)


storage_data = output.xarrays["storages"]


def storage_calculations(regions, storage_data, output):
    regionalize_data(regions, storage_data, output,
                     sectors=list(definitions.producing_sectors_name_index_dict.values()))
    regionalize_data(regions, storage_data, output,
                     sectors=list(definitions.producing_sectors_name_index_dict.values()),
                     agent_type="firm", filenamestub="baseline_relative_firm_storage_",
                     aggregate_regions=False)


print("Crunching numbers for single regions...")


def compute_tasklist(tasklist):
    dask.compute(*tasklist)


tasks = []
print("Storages...")

# aggregated storage data to check consumption patterns
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    aggregated_storage_data = helpers.aggregate_by_sector_group(storage_data, definitions.consumption_baskets)
baseline_aggregated_storage_data = baseline_relative(aggregated_storage_data)
if args.regions[0] == "ALL":
    baseline_aggregated_storage_data.persist()

baseline_storage_data = baseline_relative(storage_data)
if args.regions[0] == "ALL":
    baseline_storage_data.persist()
storage_files = regionalize_data(args.regions, baseline_storage_data, output,
                                 sectors=list(definitions.producing_sectors_name_index_dict.values()),
                                 data_baseline_relative=True)
tasks.append(safe_file_dict(storage_files, args.outputdir))
aggregated_storage_files = regionalize_data(args.regions, baseline_aggregated_storage_data, output, sectors=[0, 1, 2],
                                            filenamestub="baseline_relative_consumer_basket_storage_",
                                            data_baseline_relative=True)
tasks.append(safe_file_dict(aggregated_storage_files, args.outputdir))
print("Consumers...")
# # calculate consumer data
consumer_data = output.xarrays["consumers"]
if args.regions[0] == "ALL":
    consumer_data.persist()
consumer_files = regionalize_data(args.regions, consumer_data, output,
                                  sectors=None, filenamestub="baseline_relative_consumer_data_")
tasks.append(safe_file_dict(consumer_files, args.outputdir))
print("Firms...")
# # calculate firm data
firm_data = output.xarrays["firms"]
if args.regions[0] == "ALL":
    firm_data.persist()
firm_files = regionalize_data(args.regions, firm_data, output,
                              sectors=None, filenamestub="baseline_relative_firm_data_", aggregate_regions=False)
tasks.append(safe_file_dict(firm_files, args.outputdir))

# call compute just once to allow maximum optimization by dask scheduler
tasks = [i_task for tasklist in tasks for i_task in tasklist]
compute_tasklist(tasks)

print("aggregated region statistics are calculated...")
region_data = output.xarrays["regions"]
with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    baseline_region_data = baseline_relative(region_data)
    baseline_region_data = baseline_region_data.compute()
baseline_region_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))

print("Done.")
