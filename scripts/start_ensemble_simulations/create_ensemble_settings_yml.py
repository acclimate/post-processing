"""
This file provides an example of how to generate a set of settings files for running an ensemble of model configurations,
based on a template settings file, and a set of parameters, or input files for e.g. model forcing, to vary.
"""

import os
import re
import ruamel.yaml as yaml
import xarray as xr

ensembleidentifier = "example_ensemble"
basedir = "/p/projects/acclimate/projects/heated_productivity"

forcingpath = os.path.join(basedir, "data","input","forcing")

#collect list of forcing files to include in the ensemble using example filename
example_forcing_file = "impacts_ssp126_cnrm-cm6-1_2080-2089_preday.nc"

# Define regex pattern to match the example forcing file
pattern = re.compile(r"impacts_([a-zA-Z0-9-]+)_([a-zA-Z0-9-]+)_(\d{4}-\d{4})_preday\.nc")

# Collect all forcing files that match the pattern, and create a dictionary of forcing names = filename without extension, and forcing file path
forcingdict = {}
for root, dirs, files in os.walk(forcingpath):
    for file in files:
        match = pattern.match(file)
        if match:
            forcingname = file.split(".")[0]
            forcingdict[forcingname] = os.path.join(root, file)


with open(os.path.join(basedir, "forcingdict.yml"), "w") as file:
        # yaml.dump need a dict and a file handler as parameter
        yaml = yaml.YAML()
        yaml.indent(sequence=4, offset=2)
        yaml.dump(forcingdict, file)

forcingdict = yaml.load(open(os.path.join(basedir, "forcingdict.yml")))

for subdir in ["settings", "runs"]:
    dir_path = os.path.join(basedir, subdir)
    os.makedirs(dir_path, exist_ok=True)

list_of_settings = []

settings_template = os.path.join(basedir,"settings_template.yml")

settingsdir = os.path.join(basedir,"settings","acclimate_settings")

for forcing in forcingdict:
    forcingname = forcing
    forcingfile = forcingdict[forcingname]
    basedate = xr.open_dataset(forcingfile).time[0].values
    #convert basedate to string of YYYY-MM-DD
    basedate = str(basedate).split("T")[0]
    number_of_days = len(xr.open_dataset(forcingfile).time)

    
    identifier = f"{forcingname}"
    simulationdir = os.path.join(basedir,"data","output","runs")
    os.makedirs(simulationdir, exist_ok=True)

    settingsfile = os.path.join(settingsdir, f"{identifier}.yml")
    outputfile = os.path.join(simulationdir, f"{identifier}.nc")

    with open(settings_template, 'r') as f:
        settings = yaml.load(f)

    settings["outputs"][1]["file"] = outputfile
    forcing_settings = settings["scenario"]
    forcing_settings["stop"] = number_of_days
    forcing_settings["basedate"] = basedate
    forcing_settings["forcing"]["file"] = forcingfile

    with open(settingsfile, 'w') as f:
        yaml.dump(settings, f)

    settingsfile = os.path.join(settingsdir, f"{identifier}.yml")
    list_of_settings.append(settingsfile)


with open(os.path.join(settingsdir, f"settings_{ensembleidentifier}.yml"), 'w') as f:
    yaml.dump(list_of_settings, f)