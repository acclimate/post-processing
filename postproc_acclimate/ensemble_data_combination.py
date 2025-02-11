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

def process_datadict_to_datatree(datadict, group_selection, group_variables, data_agent_converter=None):
    """
    Process a datadict of grouped netcdf files to create a nested DataTree structure.
    
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

def process_datadict_to_datasets(datadict, variable_selection=None):
    """
    Process a dictionary of NetCDF files into one ensemble dataset.

    This function processes a dictionary (organized by model, SSP, and time period) of NetCDF files 
    without groups into one ensemble dataset. It reads the NetCDF files, optionally selects specific 
    variables, and adds metadata for model, SSP, and time period before combining them into a single 
    xarray dataset.

    Args:
        datadict (dict): A nested dictionary where the first level keys are model names, the second 
                         level keys are SSP names, and the third level keys are time periods. The 
                         values are file paths to the NetCDF files.
        variable_selection (str or list of str, optional): A variable or list of variables to select 
                                                           from the NetCDF files. If None, all 
                                                           variables are selected. Defaults to None.

    Returns:
        xarray.Dataset: An xarray dataset containing the combined data from all the NetCDF files, 
                        with additional coordinates for model, SSP, and time period.

    Example:
        datadict = {
            'model1': {
                'ssp1': {
                    '2020-2030': 'path/to/netcdf1.nc',
                    '2030-2040': 'path/to/netcdf2.nc'
                },
                'ssp2': {
                    '2020-2030': 'path/to/netcdf3.nc'
                }
            },
            'model2': {
                'ssp1': {
                    '2020-2030': 'path/to/netcdf4.nc'
                }
            }
        }
        combined_dataset = process_datadict_to_datasets(datadict, variable_selection='temperature')
    """
    datalist = []
    for model in datadict:
        for ssp in datadict[model]:
            for timeperiod in datadict[model][ssp]:
                data = xr.open_dataset(datadict[model][ssp][timeperiod])
                if variable_selection is not None:
                    data = data[variable_selection]
                data["model"] = model
                data["ssp"] = ssp
                data["timeperiod"] = timeperiod
                data = data.expand_dims(["model", "ssp", "timeperiod"])
                data = data.set_coords(["model", "ssp", "timeperiod"])
                datalist.append(data)
    return xr.combine_by_coords(datalist)
                

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


def create_ensemble_dataset(basedir, ssp_globterm="ssp[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", modellist=None, recursive=True, variable_selection=None):
        """ Create an ensemble dataset by combining NetCDF files from a specified base directory.
        This function searches for NetCDF files in the given base directory that match specified patterns for SSP and time period.
        It then combines the data from these files into a single xarray dataset.
        
        Args:
            basedir (str): The base directory to search for files.
            ssp_globterm (str, optional): The glob pattern for SSP. Defaults to "ssp[0-9][0-9][0-9]".
            time_globterm (str, optional): The glob pattern for time period. Defaults to "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]".
            modellist (list, optional): List of model names to search for. Defaults to None.
            recursive (bool, optional): Whether to search directories recursively. Defaults to True.
            variable_selection (str or list of str, optional): A variable or list of variables to select from the NetCDF files. Defaults to None, which implies all variables are processed.
        
        Returns:
            xarray.Dataset: An xarray dataset containing the combined data from all the NetCDF files.
        """
        datadict = find_ensemble_files(basedir, ssp_globterm, time_globterm, modellist, recursive)
        combined_dataset = process_datadict_to_datasets(datadict, variable_selection)
        return combined_dataset

def create_ensemble_datatree(basedir, group_selection, group_variables, modellist=None, ssp_globterm="ssp[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", recursive=True, data_agent_converter=None):
    """
    Create an ensemble DataTree from a base directory containing grouped NetCDF files.
    
    This function searches for NetCDF files in the given base directory that match specified patterns for SSP and time period.
    Groups are selected from the raw output based on the group_selection list, and specific variables are retained for each group based on the group_variables dictionary.
    It then processes these files into a nested DataTree structure, organizing the data by model, SSP, and time period.
    
    Args:
        basedir (str): The base directory to search for files.
        group_selection (list): List of groups to select from the raw output. Each group corresponds to a specific section of the NetCDF file.
        group_variables (dict): Dictionary specifying variables to keep for each group. The keys are group names, and the values are lists of variable names to retain. Use "ALL" to keep all variables in a group.
        modellist (list, optional): List of model names to search for. Defaults to None.
        ssp_globterm (str, optional): The glob pattern for SSP. Defaults to "ssp[0-9][0-9][0-9]".
        time_globterm (str, optional): The glob pattern for time period. Defaults to "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]".
        recursive (bool, optional): Whether to search directories recursively. Defaults to True.
        data_agent_converter (function, optional): Function to convert raw output data agent dimension. Defaults to None.
    
    Returns:
        xr.DataTree: A nested DataTree structure containing the processed data.
    
    Example:
        basedir = "/path/to/data"
        group_selection = ["group1", "group2"]
        group_variables = {
            "group1": ["var1", "var2"],
            "group2": "ALL"
        }
        datatree = create_ensemble_datatree(basedir, group_selection, group_variables)
    """
    datadict = find_ensemble_files(basedir, ssp_globterm, time_globterm, modellist, recursive)
    combined_datatree = process_datadict_to_datatree(datadict, group_selection, group_variables, data_agent_converter)
    return combined_datatree
