#!/usr/bin/env python3
import pandas as pd
import xarray as xr
from netCDF4 import Dataset
import numpy as np


class AcclimateOutput:
    def __init__(self, filename=None, start_date=None, data=None, baseline=None, old_output_format=False,
                 groups_to_load=None, vars_to_load=None):
        if data is not None and filename is None:
            self._data = data
            self._baseline = baseline
        elif filename is not None:
            self._data = xr.Dataset()
            self._baseline = xr.Dataset()
            if groups_to_load is None:
                groups_to_load = ['firms', 'regions', 'storages', 'consumers']
            self.load_dataset(filename=filename, start_date=start_date, groups_to_load=groups_to_load,
                              vars_to_load=vars_to_load)
        else:
            raise ValueError("Either both or none of data and filename were passed.")

    @property
    def data(self):
        return self._data

    @property
    def baseline(self):
        return self._baseline

    def load_dataset(self, filename, start_date, groups_to_load, vars_to_load=None):
        for i_group in groups_to_load:
            try:
                with xr.open_dataset(filename, group=i_group, chunks="auto") as _data:
                    if vars_to_load is not None:
                        _data = _data[vars_to_load]
                    _data = _data.rename({v: "{}_{}".format(i_group, v) for v in list(_data.variables)})
                    self._data.update(_data)
            except OSError as e:
                print("OS error: {0}".format(e))
        with Dataset(filename, 'r') as ncdata:
            agent_names = [str(a[0].decode('UTF-8')) for a in ncdata['agent'][:]]
            agent_types = [ncdata['agent_type'][a[1]] for a in ncdata['agent'][:]]
            agent_sectors = [ncdata['sector'][a[2]] for a in ncdata['agent'][:]]
            agent_regions = [ncdata['region'][a[3]] for a in ncdata['agent'][:]]
            for idx in range(len(agent_names)):
                agent_names[idx] = agent_names[idx].split(':')[0] + ":{}".format(agent_regions[idx])
            if start_date is None:
                start_date = ncdata['time'].units.split(' ')[-1]
            coords = {
                'time': pd.date_range(start_date, periods=len(ncdata['time']), freq='D'),
                'agent': agent_names,
                'agent_region': ('agent', agent_regions),
                'agent_sector': ('agent', [s if t == 'firm' else 'FCON' for s, t in zip(agent_sectors, agent_types)]),
                'agent_type': ('agent', agent_types),
                'region': ncdata['region'][:],
            }
        self._data = self._data.assign_coords(coords)
        self._baseline = self._data.sel(time=self._data.time[0])

    def get_agents(self, sector=None, region=None, agent_type=None):
        if 'agent' in self:
            def selector(v, lookup):
                if v is not None:
                    return np.isin(lookup, v)
                else:
                    return np.array([True] * len(lookup))

            return self['agent'].values[selector(sector, self['agent_sector'].values) &
                                        selector(region, self['agent_region'].values) &
                                        selector(agent_type, self['agent_type'].values)]
        else:
            print("No dimension 'agent'")

    def sel(self, inplace=False, **kwargs):
        if 'agent' in self.coords:
            agent_sel = self.agent.values
            num_directly_selected_agents = 0
            if 'agent' in kwargs:
                agent_sel = kwargs['agent']
                if type(agent_sel) == str:
                    agent_sel = [agent_sel]
                num_directly_selected_agents = len(agent_sel)
            agent_subindex_keys = list({'region', 'sector', 'agent_region', 'agent_sector', 'agent_type'} & set(kwargs.keys()))
            agent_sel = np.intersect1d(agent_sel, self.get_agents(**{k.replace('agent_', '') if k != 'agent_type' else k: kwargs[k] for k in agent_subindex_keys}))
            if len(agent_sel) == 0:
                print("\nWarning. Values passed for arguments {} are contradictory.".format(set(kwargs.keys())) +
                      " No agents left with this selection.\n")
            elif num_directly_selected_agents > len(agent_sel):
                print("\nWarning. Values passed for arguments {} are contradictory.\n".format(set(kwargs.keys())))
            kwargs['agent'] = agent_sel
            for key in agent_subindex_keys:
                if key in ['agent_region', 'agent_sector', 'agent_type']:
                    kwargs.pop(key)
        if inplace:
            self._data = self._data.sel(**kwargs)
            self._baseline = self._baseline.sel(**kwargs)
        else:
            return AcclimateOutput(data=self._data.sel(**kwargs),
                                   baseline=self._baseline.sel(**{k: v for k, v in kwargs.items() if k != 'time'}))

    def group_agents(self, dim: str, mapping: dict = None, how: str = 'sum', drop: bool = False, inplace: bool = False):
        if dim not in ['region', 'sector']:
            raise ValueError("Cannot group along dimension {}".format(dim))
        coords = {c: ('agent', c_.values) for c, c_ in self.coords.items() if c in
                  ['agent', 'agent_sector', 'agent_region', 'agent_type']}
        for k, v in mapping.items():
            if k in self.__getattr__('agent_' + dim):
                print("{} already exists in dimension {}".format(k, 'agent_' + dim))
                continue
            vals_to_group = [_v for _v in v if _v in self['agent_' + dim]]
            if len(vals_to_group) == 0:
                print("One or more of the values for new entry '{}' could not be found in dim '{}'".format(k, dim))
            elif len(vals_to_group) < len(v):
                print("None of the values for new entry '{}' could not be found in dim '{}'".format(k, dim))
            agents_to_group = self.get_agents(**{dim: vals_to_group})
            mixed_agent_types = (dim == 'sector' and len(v) > 1 and 'FCON' in v)
            for a in agents_to_group:
                agent_index = np.where(self['agent'].values == a)[0][0]
                if dim == 'sector':
                    coords['agent'][1][agent_index] = "{}:{}".format(a.split(':')[1], k)
                    coords['agent_sector'][1][agent_index] = k
                elif dim == 'region':
                    coords['agent'][1][agent_index] = "{}:{}".format(k, a.split(':')[0])
                    coords['agent_region'][1][agent_index] = k
                if mixed_agent_types:
                    coords['agent_type'][1][agent_index] = 'mixed'
        new_coords = xr.Dataset(coords).groupby('agent').first().set_coords(
            ['agent_sector', 'agent_region', 'agent_type']).coords
        if inplace:
            self._data = getattr(self._data.assign_coords(coords).groupby('agent'), how)().assign_coords(new_coords)
            self._baseline = getattr(self._baseline.assign_coords(coords).groupby('agent'), how)().assign_coords(
                new_coords)
        else:
            res_data = getattr(self._data.assign_coords(coords).groupby('agent'), how)().assign_coords(new_coords)
            res_baseline = getattr(self._baseline.assign_coords(coords).groupby('agent'), how)().assign_coords(
                new_coords)
            return AcclimateOutput(data=res_data, baseline=res_baseline)

    def _wrapper_func(self, func, inplace=False, *args, **kwargs):
        res = getattr(self._data, func)(*args, **kwargs)
        if type(res) is xr.Dataset or type(res) is xr.DataArray:
            if inplace:
                self._data = res
            else:
                return AcclimateOutput(data=res, baseline=self._baseline)
        else:
            return res

    def __repr__(self):
        _repr = super().__repr__() + "\n" + self._data.__repr__()
        _repr = _repr.replace('agent_region', '- region    ')
        _repr = _repr.replace('agent_sector', '- sector    ')
        _repr = _repr.replace('agent_type', '- type    ')
        return _repr

    def __getattr__(self, attr):
        if hasattr(self._data, attr):
            if attr in self._data:
                return AcclimateOutput(data=getattr(self._data, attr), baseline=getattr(self._baseline, attr))
            elif hasattr(getattr(self._data, attr), '__call__'):
                def res(*args, **kwargs):
                    return self._wrapper_func(attr, False, *args, **kwargs)

                res.__doc__ = getattr(self._data, attr).__doc__
            else:
                res = getattr(self._data, attr)
            return res

    def __getitem__(self, item):
        return AcclimateOutput(data=self._data[item], baseline=self._baseline[item])

    def __add__(self, other):
        return AcclimateOutput(data=self.data + (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline + other.baseline)

    def __sub__(self, other):
        return AcclimateOutput(data=self.data - (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline - other.baseline)

    def __mul__(self, other):
        return AcclimateOutput(data=self.data * (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline * other.baseline)

    def __pow__(self, other):
        return AcclimateOutput(data=self.data ** (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline ** other.baseline)

    def __truediv__(self, other):
        return AcclimateOutput(data=self.data / (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline / other.baseline)

    def __le__(self, other):
        return AcclimateOutput(data=self.data <= (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline)

    def __lt__(self, other):
        return AcclimateOutput(data=self.data < (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline)

    def __ge__(self, other):
        return AcclimateOutput(data=self.data >= (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline)

    def __gt__(self, other):
        return AcclimateOutput(data=self.data > (other.data if type(other) is self.__class__ else other),
                               baseline=self.baseline)

    def __contains__(self, item):
        data_contains = item in self._data
        baseline_contains = item in self._baseline
        if data_contains != baseline_contains:
            raise ValueError(
                "Something went wrong here. _data and _baseline objects should contain the same variables.")
        return data_contains and baseline_contains
