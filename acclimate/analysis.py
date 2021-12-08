# TODO helpers for most important quantities
import re

import holoviews as hv
import numpy as np
import xarray as xr
from scipy import signal


# some functions to calculate emerging elasticity

def baseline_calculate_empiricial_elasticities(data, classifier=re.compile('(.*)(_.*)'), tolerance=0.00001):
    variables = list(data.keys())
    # TODO: improve automatch value quantity pairs
    values = list(filter(lambda x: "value" in x, variables))
    quantities = list(filter(lambda x: "quantity" in x, variables))
    quantity_value_pairs = list(zip(quantities, values))
    try:
        for i_pair in quantity_value_pairs:
            identifier = classifier.match(i_pair[0])[1]
            quantity_change = data[i_pair[0]] - 1
            quantity_change = quantity_change.where(np.abs(quantity_change) > tolerance)
            price_change = (data[i_pair[1]] / data[i_pair[0]]) - 1
            price_change = price_change.where(np.abs(price_change) > tolerance)
            data[identifier + "_elasticity"] = quantity_change / price_change
            data[identifier + "_elasticity"] = data[identifier + "_elasticity"].where(
                np.abs(data[identifier + "_elasticity"]) <= 10)
    except Exception as e:
        print(e)
        print("no fitting data for elasticity calculation - need *_quantity and *_value variables")
    return data


def estimate_relative_change(variable, tolerance, filter=None, derivative_filter=None, filter_window=30):
    change = variable.differentiate("time")
    if filter is not None:
        filtered_variable = filter(variable, window=filter_window)
        if derivative_filter is not None:
            change = apply_filter_derivative(filtered_variable)
        else:
            change = filtered_variable.differentiate("time")
    rel_change = change / variable
    rel_change = rel_change.where(np.abs(rel_change) > tolerance)
    return rel_change


def calculate_empiricial_elasticities(dataset, filter=None, derivative_filter=None, filter_window=31,
                                      classifier=re.compile('(.*)(_.*)'), tolerance=0.00001, name="elasticity"):
    data = dataset.data
    new_kdims = []
    new_vdims = []
    try:
        variables = list(data.keys())
        # TODO: improve automatch value quantity pairs
        values = list(filter(lambda x: "value" in x, variables))
        quants = list(filter(lambda x: "quantity" in x, variables))
        quantity_value_pairs = list(zip(quants, values))
        name = "_" + name
        for i_pair in quantity_value_pairs:
            identifier = classifier.match(i_pair[0])[1]
            quantity = data[i_pair[0]]
            value = data[i_pair[1]]
            price = value / quantity
            price_change = estimate_relative_change(price, tolerance, filter=filter,
                                                    derivative_filter=derivative_filter, filter_window=filter_window)
            quantity_change = estimate_relative_change(quantity, tolerance, filter=filter,
                                                       derivative_filter=derivative_filter,
                                                       filter_window=filter_window)
            data[identifier + name] = quantity_change / price_change
            new_vdims.append(identifier + name)
            data[identifier + name + "_large_quantity_change"] = data[identifier + name].where(
                rolling_window(np.abs(quantity_change), 7, reduction_function=np.nansum) > 0.0005)
            new_vdims.append(identifier + name + "_large_quantity_change")
            data[identifier + name + "_large_price_change"] = data[identifier + name].where(
                rolling_window(np.abs(price_change), 7, reduction_function=np.nansum) > 0.0005)
            new_vdims.append(identifier + name + "_large_price_change")

    except Exception as e:
        print(e)
    kdims = dataset.kdims.append(new_kdims)
    vdims = dataset.vdims.append(new_vdims)
    return hv.Dataset(data, kdims=kdims, vdims=vdims)


# several smoothing filters


def rolling_window(data, window=31, reduction_function=np.nanmean):
    return data.rolling(time=window).reduce(reduction_function)


def rolling_window_elasticity(data):
    return calculate_empiricial_elasticities(data, filter=rolling_window,
                                             name="rolling_elasticity")


def savgol_filter(data, window=31):
    return signal.savgol_filter(data, window, 2, deriv=0)


def savgol_filter_derivative(data, window=31):
    return signal.savgol_filter(data, window, 2, deriv=1)


def apply_filter(data, filter=savgol_filter, window=31):
    return xr.apply_ufunc(filter, data, input_core_dims=[["time"]], output_core_dims=[["time"]])


def apply_filter_derivative(data, filter=savgol_filter_derivative, window=31):
    return xr.apply_ufunc(filter, data, input_core_dims=[["time"]], output_core_dims=[["time"]])


def savgol_elasticity(data):
    return calculate_empiricial_elasticities(data, filter=apply_filter, derivative_filter=apply_filter_derivative,
                                             name="savgol_elasticity", filter_window=31)


# variance, std. approximation functions for set of regions data
# TODO: ongoing development, cleanup etc. pending
def calculate_summary_stats(dataset):
    # create initial dataset:
    summary_data = dataset.std("time", skipna=True)
    for i_vdim in summary_data.variables:
        summary_data = summary_data.rename_vars(name_dict={i_vdim: "std_" + i_vdim})
    return summary_data
