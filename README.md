Acclimate post-processing
=========================

Python library and scripts for post-processing output of the `Acclimate model <https://github.com/acclimate/acclimate>`_.

Some methods may also be suitable for the general processing of ensemble simulation results in NETCDF format.

General concept
---------------

As demonstrated in ``scripts/example_simple_postproc.py``, capabilities for ensemble data processing is provided for the following steps:

1. collection of the output files into a `xarray.DataSet <http://xarray.pydata.org/en/stable/generated/xarray.Dataset.html>`_ (if dimensions are all the same) or more generally into a `xarray.DataTree <http://xarray.pydata.org/en/stable/generated/xarray.DataTree.html>`_ 
        and reorganization of data: selection of groups / variables of interest using ``data_transform.py`` and ``ensemble_data_combination.py``.
2. analysis of ensemble data: calculation of summary metrics like medians across a dimension, aggregates across dimensions, ... using ``analysis_functions.py``.
3. provision of analysis output for processing in plotting scripts as demonstrated in ``scripts/example_simple_postproc.py``.
4. small example collection of general plotting functions in ``basic-plots.py``.

Installation
------------

Please use the conda environment provided in ``compacts-simulations.yml``. 

.. code-block:: bash

        conda env update -n compacts-simulations --file compacts-simulations.yml


In this environment, the library can be installed from GitHub using pip:
Download from the respective GitHub repository with ``git@github.com:acclimate/post-processing.git`` and install the package with:

.. code-block:: bash

         pip install -e .

from within the repository to keep it up-to-date with the current state of the git repo.