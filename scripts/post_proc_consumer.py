#!/usr/bin/env python3
import argparse
import os

import dask
from dask_jobqueue import SLURMCluster

from acclimate import dataset, definitions
from acclimate import helpers


# some functions TODO: put into source file after development


def regions_consumer_firm(regions):
    results = []
    for i_region in args.regions:
        results.append(baseline_data(aggregated_storage_data, i_region, all_regions, "consumer", baskets=True))
        results.append(baseline_data(storage_data, i_region, all_regions, "consumer", production_sectors))
    return results


parser = argparse.ArgumentParser(description="Process acclimate output for easy and fast plotting.")
parser.add_argument(
    "--acclimate_output"
    , type=str,
    help="Path to the acclimate data file (default: CURRENT/output.nc)"
)
parser.add_argument(
    "--outputdir"
    , type=str,
    help="Output directory (default: CURRENT)"
)
parser.add_argument(
    "--regions"
    , type=str,
    nargs='+',
    help="Regions to analyse - if none provided, all regions from output will be used."
)

args = parser.parse_args()
all_regions = False
if not args.acclimate_output:
    args.acclimate_output = os.path.join(os.getcwd(), 'output.nc')
if not args.outputdir:
    args.outputdir = os.getcwd()

# experimental use of adaptive dask cluster to speed up operations if capacity is available on the cluster
# create dask cluster utilitzing dask.jobqueue
cluster = SLURMCluster(
    queue='standard',
    project="compacts",
    cores=16,
    memory="60 GiB",
    walltime="0-00:30:00",
    extra=["--lifetime", "25m", "--lifetime-stagger", "4m"]
)
cluster.adapt(minimum=1, maximum=10)  # ask for max 10 jobs

output = dataset.AcclimateOutput(args.acclimate_output)


@dask.delayed
def baseline_data(data, region, already_relative, agent_type, sectors=None, baskets=False, output=output):
    region_data = helpers.select_partial_data(
        helpers.select_by_agent_properties(data, output, region=region, type=agent_type),
        sector=sectors)
    filename = "baseline_relative_consumer_storage_" + region + ".nc"
    if baskets:
        filename = "baseline_relative_consumer_basket_storage_" + region + ".nc"
    if already_relative:
        region_data.to_netcdf(
            os.path.join(args.outputdir, filename))
        return filename
    else:
        region_data.map(dataset.baseline_relative).to_netcdf(
            os.path.join(args.outputdir, filename))
        return filename


if not args.regions:
    args.regions = list(output.regions)
    all_regions = False
storage_data = output.xarrays["storages"]
firm_data = output.xarrays["firms"]
consumer_data = output.xarrays["consumers"]

# aggregated storage data to check consumption patterns
aggregated_storage_data = helpers.aggregate_by_sector_group(storage_data, definitions.consumption_baskets)
production_sectors = range(0, 26)  # ignore consumption sectors
consumption_sectors = range(26, 31)

if all_regions:
    storage_data = storage_data.map(dataset.baseline_relative)
    firm_data = firm_data.map(dataset.baseline_relative)
    consumer_data = consumer_data.map(dataset.baseline_relative)
    aggregated_storage_data = aggregated_storage_data.map(dataset.baseline_relative)
    print("baseline relative data calculated.")

# calling persist keeps data in memory - speeding up output enourmously, but increasing memory requirement
aggregated_storage_data = aggregated_storage_data.persist()
storage_data = storage_data.persist()
firm_data = firm_data.persist()
consumer_data = consumer_data.persist()

# experimental use of dask delayed to parallelize
dask.compute(regions_consumer_firm(args.regions))

# get baseline relative region data
region_data = output.xarrays["regions"]
baseline_region_data = region_data.map(dataset.baseline_relative)
baseline_region_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))

# get baseline relative sector data
sector_data = output.xarrays["sectors"]
baseline_relative_sector_data = sector_data.map(dataset.baseline_relative)
baseline_relative_sector_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_sector_data.nc"))
