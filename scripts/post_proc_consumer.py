#!/usr/bin/env python3

#TODO: simplify after code overhaul


import argparse
import os

import dask
import xarray as xr
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

from acclimate import dataset, definitions, helpers

parser = argparse.ArgumentParser(description="Process acclimate output for easy and fast plotting.")
parser.add_argument(
    "--acclimate_output"
    , required=True,
    type=str,
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
        baseline_data = filtered_data
        if not data_baseline_relative:
            baseline_data = baseline_relative(filtered_data)
        filename = filenamestub + i_region + ".nc"
        files[filename] = baseline_data
    return files


def single_region_data(region, data, output, sectors=None, agent_type="consumer", aggregate_regions=True):
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        if region in definitions.WORLD_REGIONS.keys():  # filter for groups of regions
            if aggregate_regions:
                return aggregate_subregions(region, data, output, sectors=sectors, agent_type=agent_type)
            else:
                return select_storage(region, data, output, sectors=sectors,
                                      agent_type=agent_type)  # TODO: proper labelling of output to be able to filter
        else:
            return select_storage(region, data, output, sectors=sectors, agent_type=agent_type)


def aggregate_subregions(region_group, data, output, sectors=None, agent_type="consumer"):
    regions_to_aggregate = list(set(definitions.WORLD_REGIONS[region_group]) - {"BLR", "MDA", "SDN", "ZWE"})
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

from dask.distributed import progress
def compute_tasklist(tasklists):
    # experiment: track progress
    tasks = [i_task for tasklist in tasklists for i_task in tasklist]  # flatten if list of lists
    result = dask.compute(*tasks)
    progress(result)


# experimental use of adaptive dask cluster to speed up operations if capacity is available on the cluster
# create dask cluster utilitzing dask.jobqueue
cluster = SLURMCluster(
    job_extra=["--qos short", "partition=standard"],
    walltime="0-01:00:00",
    # extra=["--lifetime", "60", "--lifetime-stagger", "5m"]
)
# cluster.adapt(minimum=128, maximum=128)  # TODO: fix buggy adaptive scaling
cluster.scale(n=64)
client = Client(cluster)

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

output = dataset.AcclimateOutput(args.acclimate_output)

try:
    baseline_region_data = xr.open_dataset(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))
except:
    print("aggregated region statistics are calculated...")
    region_data = output.xarrays["regions"]
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        baseline_region_data = baseline_relative(region_data).compute()
    baseline_region_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))
try:
    baseline_sector_data = xr.open_dataset(os.path.join(args.outputdir, "baseline_relative_sector_data.nc"))
except:
    print("aggregated sector statistics are calculated...")
    sector_data = output.xarrays["sectors"]
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        baseline_sector_data = baseline_relative(sector_data).compute()
    baseline_sector_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_sector_data.nc"))

single_regions = list(set(args.regions) - set(definitions.WORLD_REGIONS.keys()))
aggregate_regions = list(set(args.regions) - set(single_regions))

print("Crunching numbers for single regions...")
print("Storages...")
storage_data = output.xarrays["storages"]
storage_files = regionalize_data(single_regions, storage_data, output,
                                 sectors=list(definitions.producing_sectors_name_index_dict.values()),
                                 data_baseline_relative=False)
aggregate_regions_storage_files = regionalize_data(aggregate_regions, storage_data, output,
                                                   sectors=list(definitions.producing_sectors_name_index_dict.values()),
                                                   data_baseline_relative=False)
tasks = [safe_file_dict(storage_files, args.outputdir)]
tasks.append(safe_file_dict(aggregate_regions_storage_files, args.outputdir))

with dask.config.set(**{'array.slicing.split_large_chunks': True}):
    aggregated_storage_data = helpers.aggregate_by_sector_group(storage_data, definitions.consumption_baskets)

aggregated_storage_files = regionalize_data(single_regions, aggregated_storage_data, output, sectors=[0, 1, 2],
                                            filenamestub="baseline_relative_consumer_basket_storage_",
                                            data_baseline_relative=False)
aggregate_regions_aggregated_storage_files = regionalize_data(aggregate_regions, aggregated_storage_data, output,
                                                              sectors=[0, 1, 2],
                                                              filenamestub="baseline_relative_consumer_basket_storage_",
                                                              data_baseline_relative=False)
tasks.append(safe_file_dict(aggregated_storage_files, args.outputdir))
tasks.append(safe_file_dict(aggregate_regions_aggregated_storage_files, args.outputdir))

compute_tasklist(tasks)

print("Consumers...")
consumer_data = output.xarrays["consumers"]
if args.regions[0] == "ALL":
    consumer_data.persist()
consumer_files = regionalize_data(args.regions, consumer_data, output,
                                  sectors=None, filenamestub="baseline_relative_consumer_data_",
                                  data_baseline_relative=False)
tasks = [safe_file_dict(consumer_files, args.outputdir)]
compute_tasklist(tasks)


print("Done.")
cluster.close()
client.close()
