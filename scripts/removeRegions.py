import argparse
import os

import xarray as xr

parser = argparse.ArgumentParser(description="remove regions from input network for acclimate.")
parser.add_argument(
    "--network"
    , type=str,
    required=True,
    help="Path to the network file (defaul"
)
parser.add_argument(
    "--outputdir"
    , type=str,
    required=True,
    help="Output directory"
)

parser.add_argument(
    "--filename"
    , type=str,
    required=True,
    help="Output filename"
)

args = parser.parse_args()

eora_network_path = args.network
xarray_network = xr.open_dataset(eora_network_path)
remove_regions = ["BLR", "MDA", "SDN", "ZWE"]
# get indizes for regions to be removed:
region_list = list(xarray_network.region.values)
removed_region_indizes = [region_list.index(i_region) for i_region in remove_regions]
combined_indizes_to_be_removed = [i_index for region_index in removed_region_indizes for i_index in
                                  range(region_index * 31, region_index * 31 + 31)]
print(combined_indizes_to_be_removed)
# remove regions with poor data quality
filtered_xarray = xarray_network  # EXPERIMENTAL a little dirty soulution: just removing indizes pointing to removed regions, such that no flows are generated from removed regions .drop_sel(region=remove_regions)
filtered_xarray.coords["index"] = ("index", filtered_xarray.index.values)
filtered_xarray = filtered_xarray.drop_sel(index=combined_indizes_to_be_removed)
# save netdcf
filtered_xarray.to_netcdf(os.path.join(args.outputdir, args.filename))
