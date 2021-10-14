#!/usr/bin/env python3
import argparse
import os

import xarray as xr

from acclimate import dataset, definitions
from acclimate import helpers

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


def aggregate_by_sector_group(data, sector_groups):
    aggregated_data = []
    for i_group in sector_groups.keys():
        sector_indizes = [definitions.producing_sectors_name_index_dict[i_sector] for i_sector in
                          sector_groups[i_group]]
        aggregate = data.sel(sector=sector_indizes).sum("sector")
        aggregated_data.append(aggregate)
    return xr.concat(aggregated_data, "sector")


args = parser.parse_args()
all_regions = False
if not args.acclimate_output:
    args.acclimate_output = os.path.join(os.getcwd(), 'output.nc')
if not args.outputdir:
    args.outputdir = os.getcwd()
output = dataset.AcclimateOutput(args.acclimate_output)


def baseline_data(data, region, already_relative, agent_type, sectors=None, output=output):
    region_data = helpers.select_partial_data(
        helpers.select_by_agent_properties(data, output, region=region, type=agent_type),
        sector=sectors)
    if already_relative:
        return region_data
    else:
        return region_data.map(dataset.baseline_relative)


if not args.regions:
    args.regions = list(output.regions)
    all_regions = True
storage_data = output.xarrays["storages"]
firm_data = output.xarrays["firms"]
consumer_data = output.xarrays["consumers"]
# aggregated storage data to check consumption patterns
aggregated_storage_data = aggregate_by_sector_group(storage_data, definitions.consumption_baskets)
production_sectors = range(0, 26)  # ignore consumption sectors
consumption_sectors = range(26, 31)

if all_regions:
    storage_data = storage_data.map(dataset.baseline_relative)
    firm_data = firm_data.map(dataset.baseline_relative)
    consumer_data = consumer_data.map(dataset.baseline_relative)
    aggregated_storage_data = aggregated_storage_data.map(dataset.baseline_relative)
    print("baseline relative data calculated.")

aggregated_storage_data = aggregated_storage_data.persist()

for i_region in args.regions:
    baseline_data(aggregated_storage_data, i_region, all_regions, "consumer").to_netcdf(
        os.path.join(args.outputdir, "baseline_relative_consumer_basket_storage_" + i_region + ".nc"))
    baseline_data(storage_data, i_region, all_regions, "consumer", production_sectors).to_netcdf(
        os.path.join(args.outputdir, "baseline_relative_consumer_storage_" + i_region + ".nc"))
    baseline_data(firm_data, i_region, all_regions, "firm").to_netcdf(
        os.path.join(args.outputdir, "baseline_relative_firms_" + i_region + ".nc"))
    baseline_data(consumer_data, i_region, all_regions, "consumer").to_netcdf(
        os.path.join(args.outputdir, "baseline_relative_consumers_" + i_region + ".nc"))

# get baseline relative region data
region_data = output.xarrays["regions"]
baseline_region_data = region_data.map(dataset.baseline_relative)
baseline_region_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_region_data.nc"))

# get baseline relative sector data
sector_data = output.xarrays["sectors"]
baseline_relative_sector_data = sector_data.map(dataset.baseline_relative)
baseline_relative_sector_data.to_netcdf(os.path.join(args.outputdir, "baseline_relative_sector_data.nc"))
