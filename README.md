toolbox-acclimate
=========================

Python package and collection of example scripts for preparing simulations and processing output of the `Acclimate model <https://github.com/acclimate/acclimate>`_.

Some methods may also be suitable for the general setup and processing of ensemble simulations.

Installation
------------

Please use the conda environment provided in ``compacts-simulations.yml``. 

.. code-block:: bash

        conda env update -n compacts-simulations --file compacts-simulations.yml


In this environment, the library can be installed from GitHub using pip:
Download from the respective GitHub repository with ``git clone https://github.com/acclimate/post-processing`` and install the package with:

.. code-block:: bash

         pip install -e .

from within the repository to keep it up-to-date with the current state of the git repo.