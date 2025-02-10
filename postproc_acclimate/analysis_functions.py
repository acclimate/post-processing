''' Methods for calculation of summary metrics on xarray ensemble data.'''

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

def get_baseline_and_aggregates(data,baseline_date,dimension,dict,new_dimension_name=None):
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

    Returns
    -------
    tuple
        Tuple with baseline aggregates and aggregates for each key in dict.
    """
    baseline_data = datatransform.get_baseline_data(data,baseline_date)
    baseline_aggregates = aggregate_by_dimension_dict(baseline_data,dimension,dict,new_dimension_name)
    aggregates = aggregate_by_dimension_dict(data,dimension,dict,new_dimension_name)
    return baseline_aggregates, aggregates