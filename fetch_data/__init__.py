"""
fetch_data
----------
A package to download data from anywhere
"""

from .catalog import read_catalog
from .core import download
from .utils import log_to_stdout


log_to_stdout()
