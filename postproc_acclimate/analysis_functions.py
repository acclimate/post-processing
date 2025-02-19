""""Methods for calculation of summary metrics on xarray ensemble data.

This script provides functions to aggregate xarray data by specified dimensions using dictionaries of keys. The main functions implemented are:
1. `aggregate_by_dimension_dict`: Aggregates data by a given dimension using a dictionary of keys. This function selects data based on the provided dictionary, sums the data along the specified dimension, and assigns new coordinates based on the dictionary keys.
2. `get_baseline_and_aggregates`: Aggregates data and provides aggregated baseline data. This function first retrieves baseline data for a specified date, then aggregates both the baseline data and the original data using the provided dictionary and dimension.


For more advanced operations and native methods in xarray, refer to the following documentation:
- [xarray apply_ufunc](https://xarray.pydata.org/en/stable/generated/xarray.apply_ufunc.html): Apply a function to xarray objects using numpy-like broadcasting and parallelized operations.
- [xarray map_blocks](https://xarray.pydata.org/en/stable/generated/xarray.map_blocks.html): Apply a function to each block of a DataArray or Dataset, enabling parallel processing and handling of larger-than-memory datasets.



"""
import xarray as xr
import postproc_acclimate.data_transform as datatransform
    
def aggregate_by_dimension_dict(data,dimension,dict,new_dimension_name=None):
    """
    Aggregate data by a given dimension using a dictionary of keys.

    Parameters
    ----------
    data : xarray.DataArray or xarray.Dataset
        The data to be aggregated.
    dimension : str
        The name of the dimension to aggregate.
    dict : dict
        A dictionary where keys are the new dimension values and values are lists of the original dimension values to be aggregated.
    new_dimension_name : str, optional
        The name of the new dimension after aggregation. If None, defaults to '{dimension}_aggregate'.

    Returns
    -------
    xarray.DataArray or xarray.Dataset
        The aggregated data with the new dimension.
    """
    if new_dimension_name is None:
        new_dimension_name = dimension+"_aggregate"
    aggregated_data = []
    for key in dict.keys():
        aggregated_data.append(data.sel({dimension:dict[key]}).sum(dim=dimension).assign_coords({new_dimension_name:key}))
    return xr.concat(aggregated_data,dim=new_dimension_name)

def get_baseline_and_aggregates(data,baseline_date,dimension,dict,new_dimension_name=None, baseline_dimension_name="time"):
    """
    Aggregate and provide aggregated baseline.

    Parameters
    ----------
    data : xarray.Dataset or xarray.DataArray
        Data to aggregate.
    baseline_date : str or datetime-like
        Baseline date.
    dimension : str
        Dimension to aggregate on.
    dict : dict
        Dictionary with key-value pairs of dimension values to aggregate.
    new_dimension_name : str, optional
        Name of the new dimension, default is dimension+"_aggregate".
    baseline_dimension_name : str, optional
        Name of the baseline dimension, default is "time".

    Returns
    -------
    tuple
        Tuple with baseline aggregates and aggregates for each key in dict.
    """

    aggregates = aggregate_by_dimension_dict(data,dimension,dict,new_dimension_name)
    baseline_aggregate = datatransform.get_baseline_data(aggregates,baseline_date,dimension=baseline_dimension_name)
    
    return baseline_aggregate, aggregates


