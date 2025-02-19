"""
This file provides an example of how to generate a set of settings files for running an ensemble of model configurations,
based on a template settings file, and a set of parameters, or input files for e.g. model forcing, to vary.
"""

import os
import re
import ruamel.yaml as yaml
import xarray as xr

# Define ensemble identifier and base directory
ensembleidentifier = "example_ensemble"
basedir = "/p/projects/acclimate/projects/heated_productivity"
# Directory containing forcing files
forcingpath = os.path.join(basedir, "data", "input", "forcing")
# Directory to store generated settings files
settingsdir = os.path.join(basedir, "settings", "acclimate_settings")

# Path to the settings template file
settings_template = os.path.join(basedir, "settings_template.yml")

# Example forcing file name to define regex pattern
example_forcing_file = "impacts_ssp126_cnrm-cm6-1_2080-2089_preday.nc"

pattern = re.compile(r"impacts_([a-zA-Z0-9-]+)_([a-zA-Z0-9-]+)_(\d{4}-\d{4})_preday\.nc")

# Collect all forcing files that match the pattern, and create a dictionary of forcing names = filename without extension, and forcing file path
forcingdict = {}
for root, dirs, files in os.walk(forcingpath):
    for file in files:
        match = pattern.match(file)
        if match:
            forcingname = file.split(".")[0]
            forcingdict[forcingname] = os.path.join(root, file)

# Save the forcing dictionary to a YAML file
with open(os.path.join(forcingpath, "forcingdict.yml"), "w") as file:
    yaml = yaml.YAML()
    yaml.indent(sequence=4, offset=2)
    yaml.dump(forcingdict, file)

# Load the forcing dictionary from the YAML file
forcingdict = yaml.load(open(os.path.join(forcingpath, "forcingdict.yml")))

# Create directories for settings and runs if they do not exist
for subdir in ["settings", "runs"]:
    dir_path = os.path.join(basedir, subdir)
    os.makedirs(dir_path, exist_ok=True)
       
def generate_individual_settings_file(forcingname, forcingfile, settings_template, settingsdir, basedir):
    """
    This function extracts the base date and number of days from the forcing file,
    updates the settings template with specific values for the current forcing file,
    and saves the updated settings to a new YAML file.

    Parameters
    ----------
    forcingname : str
        The name of the forcing.
    forcingfile : str
        The path to the forcing file.
    settings_template : str
        The path to the settings template YAML file.
    settingsdir : str
        The directory where the generated settings file will be saved.
    basedir : str
        The base directory for the simulation output.

    Returns
    -------
    str
        The path to the generated settings YAML file.
    """
    # Extract base date and number of days from the forcing file
    basedate = str(xr.open_dataset(forcingfile).time[0].values).split("T")[0]
    number_of_days = len(xr.open_dataset(forcingfile).time)

    # Define identifier and paths for simulation directory, settings file, and output file
    identifier = f"{forcingname}"
    simulationdir = os.path.join(basedir, "data", "output", "runs")
    os.makedirs(simulationdir, exist_ok=True)

    settingsfile = os.path.join(settingsdir, f"{identifier}.yml")
    outputfile = os.path.join(simulationdir, f"{identifier}.nc")

    # Load the settings template
    with open(settings_template, 'r') as f:
        settings = yaml.load(f)

    # Update settings with specific values for the current forcing file
    settings["outputs"][1]["file"] = outputfile
    forcing_settings = settings["scenario"]
    forcing_settings["stop"] = number_of_days
    forcing_settings["basedate"] = basedate
    forcing_settings["forcing"]["file"] = forcingfile

    # Save the updated settings to a new YAML file
    with open(settingsfile, 'w') as f:
        yaml.dump(settings, f)

    return settingsfile

list_of_settings = [generate_individual_settings_file(forcingname, forcingfile, settings_template, settingsdir, basedir) for forcingname, forcingfile in forcingdict.items()]

# Save the list of settings files to a YAML file
with open(os.path.join(settingsdir, f"settings_{ensembleidentifier}.yml"), 'w') as f:
    yaml.dump(list_of_settings, f)