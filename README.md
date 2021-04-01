fetch-data
==============================
[![Build Status](https://github.com/lukegre/fetch-data/workflows/Tests/badge.svg)](https://github.com/lukegre/fetch-data/actions)
[![codecov](https://codecov.io/gh/lukegre/fetch-data/branch/master/graph/badge.svg)](https://codecov.io/gh/lukegre/fetch-data)
[![License:MIT](https://img.shields.io/badge/License-MIT-lightgray.svg?style=flt-square)](https://opensource.org/licenses/MIT)[![pypi](https://img.shields.io/pypi/v/fetch-data.svg)](https://pypi.org/project/fetch-data)
<!-- [![conda-forge](https://img.shields.io/conda/dn/conda-forge/fetch-data?label=conda-forge)](https://anaconda.org/conda-forge/fetch-data) -->[![Documentation Status](https://readthedocs.org/projects/fetch-data/badge/?version=latest)](https://fetch-data.readthedocs.io/en/latest/?badge=latest)


Download remote data (HTTP, FTP, SFTP) and store locally for data pipeline.

This package was created out of the frustration that it is very difficult to download data easily with `intake`.
`fetch-data` is a mash-up of `fsspec` and `pooch` making it easy to download multiple files and store all the info, making it good for data pipeline applications.


Installation
------------
Currently, this package is
`pip install git+https://github.com/lukegre/fetch-data.git`


Basic usage
-----------

Use the download function directly:

```python
flist = fd.download(url)
```

The file will be downloaded to the current directory and will be populated with a readme file, cached file list, and logging information.


Using with YAML catalogs
------------------------
Use the catalog YAML entry
```python
cat = df.read_catalog(cat_fname)
flist = fd.download(**cat['entry_name'])
```

The catalog should be structured as shown below:
```yaml
entry_name:
    url: remote path to file/s. Can contain *
    dest: where the file/s will be stored - can have optional {} placeholders that will be replaced
    meta:  # this will be written to the README file
        doi: url to the data source
        description: info about the data
        citation: how to cite this dataset
    placeholder: value  # optional will replace values in dest
```

--------

<p><small>Project based on the <a target="_blank" href="https://github.com/jbusecke/cookiecutter-science-project">cookiecutter science project template</a>.</small></p>
