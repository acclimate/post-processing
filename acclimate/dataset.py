#!/usr/bin/env python3
import pandas as pd
import xarray as xr
from netCDF4 import Dataset


# TODO: implement baseline values --> should be separated from the rest of the data s.th. it does not get lost upon
#       modification of the data itself; any shape modification (e.g. region filtering) should be applied to the
#       baseline too, though
class AcclimateOutput(xr.Dataset):
    def __init__(self, filename, start_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for i_group in ['firms', 'regions']:
            try:
                with xr.open_dataset(filename, group=i_group) as _data:
                    _data = _data.rename({v: "{}_{}".format(i_group, v) for v in list(_data.variables)})
                    self.update(_data)
            except OSError as e:
                print("OS error: {0}".format(e))
        with Dataset(filename, 'r') as ncdata:
            if start_date is None:
                start_date = ncdata['time'].units.split(' ')[-1]
            agent_var = ncdata['agent'][:]
            region_var = ncdata['region'][:]
            sector_var = ncdata['sector'][:]
            agent_type_var = ncdata['agent_type'][:]
            coords = {
                'agent': pd.MultiIndex.from_tuples(
                    zip(
                        [region_var[a['region']] for a in agent_var],
                        [sector_var[a['sector']] if a['agent_type'] == 0 else 'FCON' for a in agent_var],
                        [agent_type_var[a['agent_type']] for a in agent_var],
                        [a['name'].decode('UTF-8') for a in agent_var]
                    ),
                    names=['agent_region', 'agent_sector', 'agent_type', 'agent_name']
                ),
                'region': region_var,
                'sector': sector_var,
                'time': pd.date_range(start_date, periods=len(ncdata['time']), freq='D'),
            }
        for coord, ticks in coords.items():
            self[coord] = ticks

    def sel(self, **kwargs):
        if 'region' in kwargs and 'agent_region' in kwargs:
            raise ValueError('Can only specify one of region and agent_region')
        if 'sector' in kwargs and 'agent_sector' in kwargs:
            raise ValueError('Can only specify one of sector and agent_sector')
        if 'region' in kwargs or 'agent_region' in kwargs:
            if 'region' in kwargs:
                kwargs['agent_region'] = kwargs['region']
            else:
                kwargs['region'] = kwargs['agent_region']
        if 'sector' in kwargs or 'agent_sector' in kwargs:
            if 'sector' in kwargs:
                kwargs['agent_sector'] = kwargs['sector']
            else:
                kwargs['sector'] = kwargs['agent_sector']
            if kwargs['sector'] == 'FCON':
                kwargs['agent_type'] = 'consumer'
            else:
                kwargs['agent_type'] = 'firm'
        return super().sel(**kwargs)
