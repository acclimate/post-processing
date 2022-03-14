#!/usr/bin/env python3
import pandas as pd
import xarray as xr
from netCDF4 import Dataset
import numpy as np


class AcclimateOutput:
    def __init__(self, filename=None, start_date=None, data=None, baseline=None, agent_coords=None,
                 agent_subcoords=None):
        if data is not None:
            if agent_coords is None or agent_subcoords is None:
                raise ValueError("Must pass agent coordinates and subcoordinates if data is passed.")
            self._data = data
            self._baseline = baseline
            self._agent_coords = agent_coords
            self._agent_subcoords = agent_subcoords
        elif filename is not None:
            self._data = xr.Dataset()
            self.load_dataset(filename=filename, start_date=start_date)
        else:
            raise ValueError("Either both or none of data and filename were passed.")

    @property
    def data(self):
        return self._data

    @property
    def baseline(self):
        return self._baseline

    @property
    def agent_coords(self):
        return self._agent_coords

    @property
    def agent_subcoords(self):
        return self._agent_subcoords

    def load_dataset(self, filename, start_date):
        for i_group in ['firms', 'regions']:
            try:
                with xr.open_dataset(filename, group=i_group) as _data:
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
            self._agent_coords = agent_names
            self._agent_subcoords = {
                'region': {a: r for a, r in zip(agent_names, agent_regions)},
                'sector': {a: s if t == 'firm' else 'FCON' for a, s, t in
                           zip(agent_names, agent_sectors, agent_types)},
                'agent_type': {a: t for a, t in zip(agent_names, agent_types)}
            }
            if start_date is None:
                start_date = ncdata['time'].units.split(' ')[-1]
            coords = {
                'time': pd.date_range(start_date, periods=len(ncdata['time']), freq='D'),
                'agent': agent_names,
                'region': ncdata['region'][:],
            }
        for coord, ticks in coords.items():
            self._data[coord] = ticks
        self._baseline = self._data.sel(time=self._data.time[0])

    def get_agents(self, sector=None, region=None, agent_type=None):
        def remap(l, lookupdict):
            if l is not None:
                if type(l) is str:
                    return [a for a, v in self._agent_subcoords[lookupdict].items() if v == l]
                elif type(l) is list:
                    return [a for a, v in self._agent_subcoords[lookupdict].items() if v in l]
            else:
                return list(self._agent_subcoords[lookupdict].keys())

        return list(set(remap(sector, 'sector')) & set(remap(region, 'region')) & set(remap(agent_type, 'agent_type')) &
                    set(self._data.agent.values))

    def sel(self, inplace=False, **kwargs):
        if 'agent' in self.coords:
            agent_sel = self.agent.values
            num_directly_selected_agents = 0
            if 'agent' in kwargs:
                agent_sel = kwargs['agent']
                if type(agent_sel) == str:
                    agent_sel = [agent_sel]
                num_directly_selected_agents = len(agent_sel)
            agent_subindex_keys = list({'region', 'sector', 'agent_type'} & set(kwargs.keys()))
            agent_sel = np.intersect1d(agent_sel, self.get_agents(**{k: kwargs[k] for k in agent_subindex_keys}))
            if len(agent_sel) == 0:
                print("\nWarning. Values passed for arguments {} are contradictory.".format(set(kwargs.keys())) +
                      " No agents left with this selection.\n")
            elif num_directly_selected_agents > len(agent_sel):
                print("\nWarning. Values passed for arguments {} are contradictory.\n".format(set(kwargs.keys())))
            kwargs['agent'] = agent_sel
            for key in agent_subindex_keys:
                if key not in self._data.coords:
                    kwargs.pop(key)
        if inplace:
            self._data = self._data.sel(**kwargs)
            self._baseline = self._baseline.sel(**kwargs)
        else:
            return AcclimateOutput(data=self._data.sel(**kwargs),
                                   baseline=self._baseline.sel(**{k: v for k, v in kwargs.items() if k != 'time'}),
                                   agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def _wrapper_func(self, func, inplace=False, *args, **kwargs):
        res = getattr(self._data, func)(*args, **kwargs)
        if type(res) is xr.Dataset:
            if inplace:
                self._data = res
            else:
                return AcclimateOutput(data=res, agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)
        else:
            return res

    def __repr__(self):
        _repr = self._data.__repr__()
        if 'agent' in self._data.coords:
            agent_values = self._data.agent.values
            if agent_values.shape == ():
                agent_values = [agent_values]
            subindex_repr = ""
            for k, v in self._agent_subcoords.items():
                subindex_repr += "\n   - {}\t({})".format(k, len(set([v[a] for a in agent_values])))
            index_pos = _repr.find('\n  * agent') + 2
            if _repr[index_pos:].find('\n') > 0:
                index_pos += _repr[index_pos:].find('\n')
            else:
                index_pos = len(_repr)
            return _repr[:index_pos] + subindex_repr + _repr[index_pos:]
        return _repr

    def __getattr__(self, attr):
        if hasattr(self._data, attr):
            if hasattr(getattr(self._data, attr), '__call__'):
                def res(*args, **kwargs):
                    return self._wrapper_func(func=attr, *args, **kwargs)
                res.__doc__ = getattr(self._data, attr).__doc__
            else:
                res = getattr(self._data, attr)
            return res

    def __getitem__(self, item):
        return AcclimateOutput(data=self._data[item], baseline=self._baseline[item], agent_coords=self._agent_coords,
                               agent_subcoords=self._agent_subcoords)

    def __add__(self, other):
        return AcclimateOutput(data=self.data + other.data, baseline=self.baseline + other.baseline,
                               agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def __sub__(self, other):
        return AcclimateOutput(data=self.data - other.data, baseline=self.baseline - other.baseline,
                               agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def __mul__(self, other):
        return AcclimateOutput(data=self.data * other.data, baseline=self.baseline * other.baseline,
                               agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def __pow__(self, other):
        return AcclimateOutput(data=self.data ** other.data, baseline=self.baseline ** other.baseline,
                               agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def __truediv__(self, other):
        return AcclimateOutput(data=self.data / other.data, baseline=self.baseline / other.baseline,
                               agent_coords=self._agent_coords, agent_subcoords=self._agent_subcoords)

    def __contains__(self, item):
        data_contains = item in self._data
        baseline_contains = item in self._baseline
        if data_contains != baseline_contains:
            raise ValueError("Something went wrong here. _data and _baseline objects should contain the same variables.")
        return data_contains and baseline_contains
