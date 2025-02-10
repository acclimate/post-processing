''' Methods for data transformation of xarray ensemble data.'''

import xarray as xr


def get_baseline_data(data,baseline_date):
    """
    Get baseline data for a given date.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The input data from which to extract the baseline.
    baseline_date : str or pandas.Timestamp
        The date for which the baseline data is to be selected.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The baseline data corresponding to the given date.
        
    """
    return data.sel(time=baseline_date)

def add_region_sector(data):
    """
    Adds 'sector' and 'region' coordinates to the input xarray DataArray based on the 'agent' dimension,
    and then groups the data by 'sector' and 'region', summing the values within each group.
    Parameters
    ----------
    data : xarray.DataArray
        The input data array which must have an 'agent' dimension. The 'agent' values should be strings
        formatted as 'sector:region'.
    Returns
    -------
    xarray.DataArray
        The transformed data array with 'sector' and 'region' coordinates added, and values summed
        within each 'sector' and 'region' group.
    Notes
    -----
    This function assumes that the 'agent' values in the input data array are strings formatted as
    'sector:region'. The function splits these strings to create new 'sector' and 'region' coordinates,
    and then groups and sums the data accordingly.
    Examples
    --------
    >>> import xarray as xr
    >>> data = xr.DataArray(
    ...     [1, 2, 3, 4],
    ...     dims="agent",
    ...     coords={"agent": ["A:1", "A:2", "B:1", "B:2"]}
    ... )
    >>> result = add_region_sector(data)
    >>> print(result)
    <xarray.DataArray (sector: 2, region: 2)>
    array([[1, 2],
           [3, 4]])
    Coordinates:
      * sector   (sector) <U1 'A' 'B'
      * region   (region) <U1 '1' '2'
    """
    if "agent" in data.dims:
        sector = xr.DataArray([agent.split(":")[0] for agent in data.agent.values], dims="agent")
        region = xr.DataArray([agent.split(":")[1] for agent in data.agent.values], dims="agent")
        
        data = data.assign_coords(sector=sector, region=region)
        data_transformed = data.groupby("sector").map(lambda x: x.groupby("region").sum())
        data_transformed = data_transformed.chunk('auto')
    return data_transformed

