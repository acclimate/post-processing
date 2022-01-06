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


def estimate_relative_change(variable, tolerance, value_filter=None, derivative_filter=None, filter_window=30):
    change = variable.differentiate("time")
    if value_filter is not None:
        filtered_variable = value_filter(variable, window=filter_window)
        if derivative_filter is not None:
            change = derivative_filter(filtered_variable)
        else:
            change = filtered_variable.differentiate("time")
    rel_change = change / variable
    rel_change = rel_change.where(np.abs(rel_change) > tolerance)
    return rel_change


def calculate_empiricial_elasticities(dataset, value_filter=None, derivative_filter=None, filter_window=31,
                                      classifier=re.compile('(.*)(_.*)'), tolerance=0.0000001, name="elasticity",
                                      absolute_cap=1.5):
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
            price_change = estimate_relative_change(price, tolerance, value_filter=value_filter,
                                                    derivative_filter=derivative_filter, filter_window=filter_window)
            quantity_change = estimate_relative_change(quantity, tolerance, value_filter=value_filter,
                                                       derivative_filter=derivative_filter,
                                                       filter_window=filter_window)
            data[identifier + name] = quantity_change / price_change
            data[identifier + name + "_price_change"] = price_change
            data[identifier + name + "_quantity_change"] = quantity_change

            # dynamic data filtering based on sector 2 # ONLY REASONABLE FOR BASKET DATA
            max_at_cap = np.nanpercentile(np.abs(price_change.sel(sector=2).where(
                np.abs(data.sel(sector=2)[identifier + name]) > absolute_cap)), 100)
            data["max_price_change_cap"] = price_change.where(
                np.abs(price_change) < 0, max_at_cap, drop=False)
            data[identifier + name + "_price_change_capped_dynamic"] = data[identifier + name + "_price_change"].where(
                np.abs(price_change) >= max_at_cap)
            data[identifier + name + "_quantity_change_capped_dynamic"] = data[
                identifier + name + "_quantity_change"].where(
                np.abs(price_change) >= max_at_cap)
            data[identifier + name + "_capped_dynamic"] = data[identifier + name].where(
                np.abs(price_change) >= max_at_cap)

    except Exception as e:
        print(e)
    return hv.Dataset(data)


# several smoothing filters


def rolling_window(data, window=31, reduction_function=np.nanmean):
    return data.rolling(time=window).reduce(reduction_function)


def rolling_window_elasticity(data):
    return calculate_empiricial_elasticities(data, value_filter=rolling_window,
                                             name="rolling_elasticity")


def savgol_filter(data, window=7):
    return signal.savgol_filter(data, window, 2, deriv=0)


def savgol_filter_derivative(data, window=7):
    return signal.savgol_filter(data, window, 2, deriv=1)


def apply_filter(data, filter=savgol_filter, window=31):
    return xr.apply_ufunc(filter, data, input_core_dims=[["time"]], output_core_dims=[["time"]])


def apply_filter_derivative(data, filter=savgol_filter_derivative, window=31):
    return xr.apply_ufunc(filter, data, input_core_dims=[["time"]], output_core_dims=[["time"]])


def savgol_elasticity(data):
    return calculate_empiricial_elasticities(data, value_filter=apply_filter, derivative_filter=apply_filter_derivative,
                                             name="savgol_elasticity", filter_window=31)


# variance, std. approximation functions for set of regions data
# TODO: ongoing development, cleanup etc. pending
def calculate_summary_stats(dataset):
    # create initial dataset:
    summary_data = dataset.std("time", skipna=True)
    for i_vdim in summary_data.variables:
        summary_data = summary_data.rename_vars(name_dict={i_vdim: "std_" + i_vdim})
    return summary_data
