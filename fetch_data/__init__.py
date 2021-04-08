"""
fetch_data
----------
A package to download data from anywhere
"""

from . import catalog as cat
from . import download as dl
from .catalog import read_catalog
from .download import download
from .utils import log_to_stdout
