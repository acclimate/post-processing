''' Methods for data combination of gridded ensemble data using xarray.'''
import os
import xarray as xr
import glob

from postproc_acclimate import definitions


def load_ensemble_files(ensembledir, pattern, group_to_load=None, group_variables=None, filetype="*.nc", recursive=False):
    """
    Load NetCDF files from a directory based on a regex pattern.
    This function loads NetCDF files from a specified directory that match a given regex pattern,
    processes the files to extract parameters from their filenames, adding these as dimensions to the data
    , and returns a list of xarray objects for combination.
    
    Parameters
    ----------
    ensembledir : str
        The directory containing the ensemble files.
    pattern : re.Pattern
        A compiled regular expression pattern with named groups to match filenames.
    group_to_load : str, optional
        The group within the NetCDF files to load. If None, the entire file is loaded.
    group_variables : list of str, optional
        The variables within the group to load. If None, all variables are loaded.
    filetype : str, optional
        The file type pattern to match (default is "*.nc").
    recursive : bool, optional
        Whether to search directories recursively (default is False).
    
    Returns
    -------
    list of xarray.Dataset
        A list of xarray objects to be merged with the appropriate xr.combine_by_coords, xr.merge, or xr.concat function.
    
    Raises
    ------
    AttributeError
        If the pattern does not match the filenames or if the named groups are not present.
    IndexError
        If the files list is empty.
    """
    if not recursive:
        files = [f for f in glob.glob(os.path.join(ensembledir, filetype)) if pattern.match(os.path.basename(f))]
    else:
        files = [f for f in glob.glob(os.path.join(ensembledir, '**', filetype), recursive=True) if pattern.match(os.path.basename(f))]
    if not files:
        raise IndexError("No files matched the given pattern.")
    
    testmatch = pattern.match(os.path.basename(files[0]))
    ensemble_parameters = testmatch.groupdict().keys()
    
    parameter_type_dict = get_parameter_types(pattern, files)

    data_to_merge = []
    for f in files:
        match = pattern.match(os.path.basename(f))
        parameters = {param: match.group(param) for param in ensemble_parameters}

        if group_to_load:
            dataset = xr.open_datatree(f, chunks='auto')[group_to_load].to_dataset()
            if group_variables:
                dataset = dataset[group_variables]
        else:
            dataset = xr.open_dataset(f, chunks='auto')
        
        # Remove dimensions without variables occuring in DataTree
        if group_to_load:
            tmp_data = xr.open_datatree(f, chunks='auto')[group_to_load].to_dataset(inherit=False)
            if group_variables:
                remaining_dims = tmp_data[group_variables].dims
            else:
                remaining_dims = tmp_data.dims
            for dim in dataset.dims:
                if dim not in remaining_dims:
                    dataset = dataset.drop_vars(dim)

        for i_param in ensemble_parameters:
            dataset[i_param] = parameters[i_param]
            dataset[i_param] = dataset[i_param].astype(parameter_type_dict[i_param])
            dataset[i_param].attrs['standard_name'] = i_param
        dataset = dataset.set_coords(list(parameters.keys())).expand_dims(list(parameters.keys()))
        data_to_merge.append(dataset)
        
    return data_to_merge


def get_parameter_types(pattern, files):
    """
    Determine the types of parameters extracted from filenames using a regex pattern.

    This function matches a regex pattern against the filenames and infers the types of the named groups
    (parameters) in the pattern. It returns a dictionary mapping parameter names to their inferred types.

    Parameters
    ----------
    pattern : re.Pattern
        A compiled regular expression pattern with named groups to match filenames.
    files : list of str
        A list of filenames to match against the pattern.

    Returns
    -------
    dict
        A dictionary where keys are parameter names and values are their inferred types (int, float, or str).

    Raises
    ------
    AttributeError
        If the pattern does not match the filenames or if the named groups are not present.
    """
    samplematch = pattern.match(os.path.basename(files[0]))
    ensemble_parameters = samplematch.groupdict().keys()
    
    def infer_type(value):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return str(value)
    
    parameter_types = {param: type(infer_type(samplematch.group(param))) for param in ensemble_parameters}
    
    return parameter_types


#TODO: consider what to keep of this specialised pipeline for model - scenario - timeperiod data
def find_ensemble_files(basedir, scenario_prefix="ssp", scenario_globpattern="[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", modellist=None, recursive=True):
    
    """
    Find and organize ensemble files based on specified patterns.
    This function searches for NetCDF files in the given base directory that match
    the specified scenario and time period patterns. It organizes the found files into
    a nested dictionary structure based on model, scenario, and time period.
    Args:
        basedir (str): The base directory to search for files.
        scenario_prefix (str, optional): The prefix for the scenario. Defaults to "ssp".
        scenario_globpattern (str, optional): The glob pattern for scenario. Defaults to "[0-9][0-9][0-9]".
        time_globterm (str, optional): The glob pattern for time period. Defaults to "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]".
        modellist (list, optional): List of model names to search for. Defaults to None, which uses a standard list of models.
        recursive (bool, optional): Whether to search directories recursively. Defaults to True.
    Returns:
        dict: A nested dictionary where the keys are model names, scenarios, and time periods,
              and the values are lists of file paths matching the criteria.
    """
    if modellist is None:
        modellist = ["UKESM1-0-LL", "GFDL-ESM4", "MPI-ESM1-2-HR", "MRI-ESM2-0", "IPSL-CM6A-LR"]  # standard isimip main models

    scenario_globterm = scenario_prefix + scenario_globpattern
    globterm = "*".join(["", time_globterm, scenario_globterm, ".nc"])
    outputfiles = glob.glob(basedir + '**' + globterm, recursive=recursive)

    datadict = {}
    for file in outputfiles:
        model = next((model for model in modellist if model in file), None)
        scenario = scenario_prefix + file.split(scenario_prefix)[1][:3]
        timeperiod = "20" + file.split(model + "-20")[1][:7]  # TODO: generalise

        if model not in datadict:
            datadict[model] = {}
        if scenario not in datadict[model]:
            datadict[model][scenario] = {}
        if timeperiod not in datadict[model][scenario]:
            datadict[model][scenario][timeperiod] = []
        datadict[model][scenario][timeperiod].append(file)
    
    return datadict


def process_datadict_to_datatree(datadict, group_selection, group_variables, data_agent_converter=None):
    """
    Process a datadict of grouped netcdf files to create a nested DataTree structure.
    
    Args:
        datadict (dict): The nested dictionary of file paths organized by model, scenario, and time period.
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
            if group_variables[group] != "ALL":
                output_dict[group] = output_dict[group][group_variables[group]]
            if "agent" in raw_output[group].dims and data_agent_converter:
                #new agent names
                new_agent_names = data_agent_converter(output_dict[group])
                #change agent names
                output_dict[group] = output_dict[group].assign_coords(agent=new_agent_names)
            if group_variables[group] != "ALL":
                remaining_dims = raw_output[group].to_dataset(inherit=False)[group_variables[group]].dims
            else:
                remaining_dims = raw_output[group].to_dataset(inherit=False).dims
            for dim in output_dict[group].dims:
                if dim not in remaining_dims:
                    output_dict[group] = output_dict[group].drop_vars(dim)
        return output_dict

    model_dict = {model: xr.DataTree.from_dict({
        scenario: xr.DataTree.from_dict({
            timeperiod: xr.DataTree.from_dict(process_file(files[0])) if len(files) == 1 else print(
                "Warning: multiple files found for model", model, "scenario", scenario, "timeperiod", timeperiod)
            for timeperiod, files in scenariodict.items()
        }) for scenario, scenariodict in modeldict.items()
    }) for model, modeldict in datadict.items()}

    return xr.DataTree.from_dict(model_dict)

def process_datadict_to_datasets(datadict, variable_selection=None):
    """
    Process a dictionary of NetCDF files into one ensemble dataset.

    This function processes a dictionary (organized by model, scenario, and time period) of NetCDF files 
    without groups into one ensemble dataset. It reads the NetCDF files, optionally selects specific 
    variables, and adds metadata for model, scenario, and time period before combining them into a single 
    xarray dataset.

    Args:
        datadict (dict): A nested dictionary where the first level keys are model names, the second 
                         level keys are scenario names, and the third level keys are time periods. The 
                         values are file paths to the NetCDF files.
        variable_selection (str or list of str, optional): A variable or list of variables to select 
                                                           from the NetCDF files. If None, all 
                                                           variables are selected. Defaults to None.

    Returns:
        xarray.Dataset: An xarray dataset containing the combined data from all the NetCDF files, 
                        with additional coordinates for model, scenario, and time period.

    Example:
        datadict = {
            'model1': {
                'scenario1': {
                    '2020-2030': 'path/to/netcdf1.nc',
                    '2030-2040': 'path/to/netcdf2.nc'
                },
                'scenario2': {
                    '2020-2030': 'path/to/netcdf3.nc'
                }
            },
            'model2': {
                'scenario1': {
                    '2020-2030': 'path/to/netcdf4.nc'
                }
            }
        }
        combined_dataset = process_datadict_to_datasets(datadict, variable_selection='temperature')
    """
    datalist = []
    for model in datadict:
        for scenario in datadict[model]:
            for timeperiod in datadict[model][scenario]:
                data = xr.open_dataset(datadict[model][scenario][timeperiod])
                if variable_selection is not None:
                    data = data[variable_selection]
                data["model"] = model
                data["scenario"] = scenario
                data["timeperiod"] = timeperiod
                data = data.expand_dims(["model", "scenario", "timeperiod"])
                data = data.set_coords(["model", "scenario", "timeperiod"])
                datalist.append(data)
    return xr.combine_by_coords(datalist)
         

def datatree_to_dataset_dict(data_tree, group_selection):
    """
    Convert a DataTree to a dictionary of Datasets by model, scenario, and time period.
    
    Args:
        data_tree (xr.DataTree): The DataTree containing the processed data.
        group_selection (list): List of groups to select from the DataTree.
        
    Returns:
        dict: A dictionary where each key is a group name and each value is a Dataset containing data 
              from all models, scenarios, and time periods for that group.
    """
    groupdata = {}
    for group in group_selection:
        mergelist = []
        models = list(data_tree.keys())
        for model in models:
            scenarios = list(data_tree[model].keys())
            for scenario in scenarios:
                timeperiods = list(data_tree[model][scenario].keys())
                for timeperiod in timeperiods:
                    data = data_tree[model][scenario][timeperiod][group]
                    data = data.to_dataset(inherit=False)
                    data["model"] = model
                    data["scenario"] = scenario
                    data["timeperiod"] = timeperiod
                    data = data.set_coords(["model", "scenario", "timeperiod"])
                    data = data.expand_dims(["model", "scenario", "timeperiod"])
                    mergelist.append(data)
        groupdata[group] = xr.merge(mergelist)
        
        #select for consumers / firms if in these groups TODO: improve Acclimate output to only give consumer / firm agents in the first place
        if group in ["consumers", "firms"]:
            agents = groupdata[group].agent.values
            regions = [agent.split(":")[1] for agent in agents]
            unique_regions = []
            for region in regions:
                if region not in unique_regions:
                    unique_regions.append(region)
            consumer_agents = [quintile+":"+region for quintile in definitions.short_quintiles for region in unique_regions]
            other_agents = [agent for agent in agents if agent not in consumer_agents]
            if group == "consumers":
                groupdata[group] = groupdata[group].sel(agent=consumer_agents)
            else:
                groupdata[group] = groupdata[group].sel(agent=other_agents)
    return groupdata


def create_ensemble_dataset(basedir, scenario_prefix="ssp",scenario_globpattern="[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", modellist=None, recursive=True, variable_selection=None):
        """ Create an ensemble dataset by combining NetCDF files from a specified base directory.
        This function searches for NetCDF files in the given base directory that match specified patterns for scenario and time period.
        It then combines the data from these files into a single xarray dataset.
        
        Args:
            basedir (str): The base directory to search for files.
            scenario_prefix (str, optional): The prefix for the scenario. Defaults to "ssp".
            scenario_globpattern (str, optional): The glob pattern for scenario. Defaults to "[0-9][0-9][0-9]".
            time_globterm (str, optional): The glob pattern for time period. Defaults to "[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]".
            modellist (list, optional): List of model names to search for. Defaults to None.
            recursive (bool, optional): Whether to search directories recursively. Defaults to True.
            variable_selection (str or list of str, optional): A variable or list of variables to select from the NetCDF files. Defaults to None, which implies all variables are processed.
        
        Returns:
            xarray.Dataset: An xarray dataset containing the combined data from all the NetCDF files.
        """
        datadict = find_ensemble_files(basedir, scenario_prefix,scenario_globpattern, time_globterm, modellist, recursive)
        combined_dataset = process_datadict_to_datasets(datadict, variable_selection)
        return combined_dataset

def create_ensemble_datatree(basedir, group_selection, group_variables, modellist=None, scenario_prefix="ssp",scenario_globpattern="[0-9][0-9][0-9]", time_globterm="[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]", recursive=True, data_agent_converter=None):
    """
    Create an ensemble DataTree from a base directory containing grouped NetCDF files.
    
    This function searches for NetCDF files in the given base directory that match specified patterns for scenario and time period.
    Groups are selected from the raw output based on the group_selection list, and specific variables are retained for each group based on the group_variables dictionary.
    It then processes these files into a nested DataTree structure, organizing the data by model, scenario, and time period.
    
    Args:
        basedir (str): The base directory to search for files.
        group_selection (list): List of groups to select from the raw output. Each group corresponds to a specific section of the NetCDF file.
        group_variables (dict): Dictionary specifying variables to keep for each group. The keys are group names, and the values are lists of variable names to retain. Use "ALL" to keep all variables in a group.
        modellist (list, optional): List of model names to search for. Defaults to None.
        scenario_prefix (str, optional): The prefix for the scenario. Defaults to "ssp".
        scenario_globpattern (str, optional): The glob pattern for scenario. Defaults to "[0-9][0-9][0-9]".
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
    datadict = find_ensemble_files(basedir, scenario_prefix,scenario_globpattern, time_globterm, modellist, recursive)
    combined_datatree = process_datadict_to_datatree(datadict, group_selection, group_variables, data_agent_converter)
    return combined_datatree
