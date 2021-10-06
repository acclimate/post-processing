#!/usr/bin/env python3
# TODO: built custom wrapper for NetCDF output
import numpy as np
import xarray as xr
from netCDF4 import *

from acclimate import definitions


class AcclimateOutput:
    def __init__(self, filename):
        self.dataset = Dataset(filename)
        self.groups = self.dataset.groups
        self.__agents = self.dataset.variables["agent"][:]
        self.agents = {str(v[0]): i for i, v in enumerate(self.__agents)}
        self.sectors = {v: i for i, v in enumerate(self.dataset.variables["sector"])}
        self.regions = {v: i for i, v in enumerate(self.dataset.variables["region"])}
        for r in definitions.WORLD_REGIONS:
            if r not in self.regions:
                self.regions[r] = definitions.WORLD_REGIONS[r]
        self.agent_types = {
            v: i for i, v in enumerate(self.dataset.variables["agent_type"])
        }
        # initialize dictionary of xarrays for all groups
        self.xarrays = {}
        for i_group in self.groups:
            self.xarrays[i_group] = xr.open_dataset(
                filename, group=i_group)

    def agent(self, sector=None, region=None, type=None):
        def remap(v, lookupdict):
            if v is None:
                return lookupdict.values()
            elif isinstance(v, int):
                return [v]
            elif isinstance(v, str):
                return remap(lookupdict[v], lookupdict)
            else:
                return [k for i in v for k in remap(i, lookupdict)]

        sector = remap(sector, self.sectors)
        region = remap(region, self.regions)
        type = remap(type, self.agent_types)
        return [
            i
            for i, v in enumerate(self.__agents)
            if v[1] in type and v[2] in sector and v[3] in region
        ]

    def sum(self, var, *args, time=None):
        return np.nansum(
            self.dataset[var].__getitem__(
                tuple([slice(None, None, None) if time is None else time] + list(args))
            ),
            axis=tuple([i + 1 for i, _ in enumerate(args)]),
        )

    def get_var(self, var, *args, time=None):
        self.dataset[var].__getitem__(
            tuple([slice(None, None, None) if time is None else time] + list(args))
        )


# sketch of some helper functions to aggregate xarray Datasets
# TODO: structure - what goes here, what to helpers / analysis
def baseline_value(x, baseline_timepoint=0):
    return x.sel(time=baseline_timepoint)


# baseline, i.e. t=0, relative average:
def baseline_relative(x):
    return (x / baseline_value(x))


def time_average(x):
    return x.mean("time")
