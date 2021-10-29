# TODO helpers for most important quantities
import xarray as xr


def ensemble_data(datalist, ensemblenames=None):
    """
        Combines given data into one dataset with an extra dimension for ensemble members
          Parameters
          ----------
          datalist
            list of xarray data of single ensemble members

          Returns
          -------
          xarray.Dataset
            Dataset of the whole ensemble with extra dimension for ensemble members
        """

    if ensemblenames is None:
        ensemblenames = range(0, len(datalist))
    return xr.concat(datalist, ensemblenames, name="ensemble_member")
