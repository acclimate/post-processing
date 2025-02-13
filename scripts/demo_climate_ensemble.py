''' This script demonstrates how to load a climate ensemble from the ISIMIP bias-adjusted data and organize it into a xarray Dataset. 
'''
import xarray as xr
import re
import postproc_acclimate.ensemble_data_combination as edc

basedir = '/p/projects/isimip/isimip/ISIMIP3b/InputData/climate/atmosphere/bias-adjusted/global/daily'

#adjust search patterns based on example filename:
#example_filename = 'gfdl-esm4_r1i1p1f1_w5e5_ssp126_tas_global_daily_2021_2030.nc'
variable = 'tas'

pattern = re.compile(rf'(?P<model>[^_]+)_[^_]+_w5e5_(?P<scenario>ssp[^_]+)_({variable})_global_daily_(?P<start_year>[^_]+)_(?P<end_year>[^_]+).nc')

files = edc.load_ensemble_files(basedir, pattern,recursive=True)

ds = xr.combine_by_coords(files,combine_attrs='drop_conflicts')

print(ds)
