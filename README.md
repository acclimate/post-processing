Acclimate post-processing
=========================

Python library and scripts for post-processing output of the `Acclimate model <https://github.com/acclimate/acclimate>`.

Some methods may also be suitable for the general processing of ensemble simulation results in NETCDF format.

## General concept

As demonstrated in "scripts/example_simple_postproc.py", capabilities for ensemble data processing is provided for the following steps:

1) collection of the output files into a [xarray.DataSet](http://xarray.pydata.org/en/stable/generated/xarray.Dataset.html) (if dimensions are all the same) or more generally into a [xarray.DataTree](http://xarray.pydata.org/en/stable/generated/xarray.DataTree.html) 
and reorganization of data: selection of groups / variables of interest using `data_transform.py` and `ensemble_data_combination.py`.
3) analysis of ensemble data: calculation of summary metrics like medians across a dimension, aggregates across dimensions, ... using `analysis_functions.py`.
4) provision of analysis output for processing in plotting scripts as demonstrated in `scripts/example_simple_postproc.py`
5) small example collection of general plotting functions in `basic-plots.py`.

Installation
------------
Please use the conda environment provided in "xarray-compacts.yml".
In this environment, the library can be installed from github using pip:
Download from the respeve GitHub repostory with git@github.com:acclimate/post-processing.git. 
    Switch to the develop branch 

        git checkout develop
    
and install the package with 

        conda develop . 

from within the repository