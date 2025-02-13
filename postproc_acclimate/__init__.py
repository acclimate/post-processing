"""
Acclimate post-processing
"""
from ._version import get_versions
__version__: str = get_versions()["version"]
del get_versions

from . import _version
__version__ = _version.get_versions()['version']
