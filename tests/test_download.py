import os

import pytest

from fetch_data import download as dl
from fetch_data.catalog import read_catalog


def test_file_logging():
    import logging

    from fetch_data import utils

    dest = "./tests/downloads/logging_download.log"

    utils.log_to_file(dest)

    logging.warning("[TESTING] This is a test log for downloading")

    with open(dest) as file:
        assert "regrid" not in file.read()


def test_read_catalog():
    fname = "./tests/example_catalog.yml"

    cat = read_catalog(fname)

    assert isinstance(cat, dict)
    assert cat != {}


def test_get_url_list_no_login_http():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-SEASURFACESALINITY-L4-*_25km-*-fv2.31.nc"  # wildcards
    )

    flist = dl.get_url_list(url, use_cache=False)

    assert len(flist) != 0


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true", reason="Skipping downloads in CI"
)
@pytest.mark.parametrize(
    "method,raise_err",
    [
        ("raise_on_empty", True),
        ("raise_on_empty", False),
    ],
)
def test_get_url_list_bad_url(method, raise_err):
    url = "http://fake_url.com/test_file.nc"  # wildcards

    if raise_err:
        with pytest.raises(ValueError):
            dl.get_url_list(url, use_cache=False, raise_on_empty=raise_err)
    elif not raise_err:
        dl.get_url_list(url, use_cache=False, raise_on_empty=raise_err)


def test_get_url_list_bad_filename_raise():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/bad_file_name.nc"  # wildcards
    )

    with pytest.raises(ValueError):
        dl.get_url_list(url, use_cache=False, raise_on_empty=True)


def test_get_url_list_fake_kwarg_https():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-SEASURFACESALINITY-L4-*_25km-*-fv2.31.nc"  # wildcards
    )

    with pytest.raises(TypeError):
        dl.get_url_list(url, use_cache=False, username="tester", password="fakes")


def test_choose_downloader():
    import pooch

    url = "ftp://thispartdoesntmatter.com"

    protocol = dl.choose_downloader(url)

    assert protocol == pooch.downloaders.FTPDownloader


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true", reason="Skipping downloads in CI"
)
def test_download_urls():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-SEASURFACESALINITY-L4-*_25km-*-fv2.31.nc"
    )

    dest = "./tests/downloads/"
    urls = dl.get_url_list(
        url, cache_path=f"{dest}/remote_files.cache", use_cache=True
    )[:1]
    dl.download_urls(urls, dest_path=dest)


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true", reason="Skipping downloads in CI"
)
@pytest.mark.parametrize(
    "method,cache",
    [
        ("use_cache", False),
        ("use_cache", True),
    ],
)
def test_download_urls_save_to_subfolder(method, cache):
    import pandas as pd

    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-*_25km-2012013*-fv2.31.nc"
    )

    urls = dl.get_url_list(
        url, use_cache=cache, cache_path="./tests/downloads/remote_files.cache"
    )

    dl.download_urls(
        urls,
        dest_path="./tests/downloads/{t:%Y}",
        date_format="-%Y%m%d-",
    )


def test_make_readme():
    fname = "./tests/example_catalog.yml"

    cat = read_catalog(fname)
    for key in cat:
        dl.create_download_readme(key, **cat[key])
        break
