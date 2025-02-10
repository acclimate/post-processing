''' Methods for data combination of gridded ensemble data using xarray.'''

import xarray as xr
import glob

def find_ensemble_files(basedir, ssp_globterm="ssp[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", modellist=None, recursive=True):
    """
    Find and organize ensemble files based on specified patterns.
    This function searches for NetCDF files in the given base directory that match
    the specified SSP and time period patterns. It organizes the found files into
    a nested dictionary structure based on model, SSP, and time period.
    Args:
        basedir (str): The base directory to search for files.
        ssp_globterm (str, optional): The glob pattern for SSP. Defaults to "ssp[0-9][0-9][0-9]".
        time_globterm (str, optional): The glob pattern for time period. Defaults to "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]".
        modellist (list, optional): List of model names to search for. Defaults to None, which uses a standard list of models.
        recursive (bool, optional): Whether to search directories recursively. Defaults to True.
    Returns:
        dict: A nested dictionary where the keys are model names, SSPs, and time periods,
              and the values are lists of file paths matching the criteria.
    """
    if modellist is None:
        modellist = ["UKESM1-0-LL", "GFDL-ESM4", "MPI-ESM1-2-HR", "MRI-ESM2-0", "IPSL-CM6A-LR"]  # standard isimip main models


    globterm = "*".join(["", time_globterm, ssp_globterm, ".nc"])
    outputfiles = glob.glob(basedir + '**' + globterm, recursive=recursive)

    datadict = {}
    for file in outputfiles:
        model = next((model for model in modellist if model in file), None)
        ssp = "ssp" + file.split("ssp")[1][:3]
        timeperiod = "20" + file.split(model + "-20")[1][:7]  # TODO: generalise

        if model not in datadict:
            datadict[model] = {}
        if ssp not in datadict[model]:
            datadict[model][ssp] = {}
        if timeperiod not in datadict[model][ssp]:
            datadict[model][ssp][timeperiod] = []
        datadict[model][ssp][timeperiod].append(file)
    
    return datadict

def process_datadict(datadict, group_selection, group_variables, data_agent_converter=None):
    """
    Process the datadict to create a nested DataTree structure.
    
    Args:
        datadict (dict): The nested dictionary of file paths organized by model, SSP, and time period.
        group_selection (list): List of groups to select from the raw output.
        group_variables (dict): Dictionary specifying variables to keep for each group.
        data_agent_converter (function, optional): Function to convert raw output data agent dimension. Defaults to None.
    
    Returns:
        xr.DataTree: A nested DataTree structure containing the processed data.
    """
    def process_file(file):
        raw_output = xr.open_datatree(file)
        output_dict = {}
        for group in group_selection:
            output_dict[group] = raw_output[group].to_dataset()
            if "agent" in raw_output[group].dims and data_agent_converter:
                output_dict[group]["agent"] = data_agent_converter(raw_output[group].to_dataset())
                output_dict[group] = output_dict[group].reindex(agent=output_dict[group]["agent"])
            if group_variables[group] != "ALL":
                remaining_dims = raw_output[group].to_dataset(inherit=False)[group_variables[group]].dims
            else:
                remaining_dims = raw_output[group].to_dataset(inherit=False).dims
            for dim in output_dict[group].dims:
                if dim not in remaining_dims:
                    output_dict[group] = output_dict[group].drop_vars(dim)
        return output_dict

    model_dict = {model: xr.DataTree.from_dict({
        ssp: xr.DataTree.from_dict({
            timeperiod: xr.DataTree.from_dict(process_file(files[0])) if len(files) == 1 else print(
                "Warning: multiple files found for model", model, "ssp", ssp, "timeperiod", timeperiod)
            for timeperiod, files in sspdict.items()
        }) for ssp, sspdict in modeldict.items()
    }) for model, modeldict in datadict.items()}

    return xr.DataTree.from_dict(model_dict)


def datatree_to_dataset(data_tree, group_selection):
    """
    Convert a DataTree to a Dataset by model, ssp, and timeperiod.
    
    Args:
        data_tree (xr.DataTree): The DataTree containing the processed data.
        group_selection (list): List of groups to select from the DataTree.
    Returns:
        xr.Dataset: A merged Dataset containing data from all models, SSPs, and time periods for each of the selected groups.
    """
    groupdata = {}
    for group in group_selection:
        mergelist = []
        models = list(data_tree.keys())
        for model in models:
            ssps = list(data_tree[model].keys())
            for ssp in ssps:
                timeperiods = list(data_tree[model][ssp].keys())
                for timeperiod in timeperiods:
                    data = data_tree[model][ssp][timeperiod][group]
                    data = data.to_dataset(inherit=False)
                    data["model"] = model
                    data["ssp"] = ssp
                    data["timeperiod"] = timeperiod
                    data = data.set_coords(["model", "ssp", "timeperiod"])
                    data = data.expand_dims(["model", "ssp", "timeperiod"])
                    mergelist.append(data)
        groupdata[group] = xr.merge(mergelist)
    return groupdata
    