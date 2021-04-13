import os

import pytest

import fetch_data as fd


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

    cat = fd.read_catalog(fname)

    assert isinstance(cat, dict)
    assert cat != {}


def test_get_url_list_no_login_http():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-SEASURFACESALINITY-L4-*_25km-*-fv2.31.nc"  # wildcards
    )

    flist = fd.core.get_url_list(url, use_cache=False)

    assert len(flist) != 0


@pytest.mark.skipif(
    os.environ.get("CI", "false") == "true", reason="Skipping downloads in CI"
)
def test_get_url_list_bad_url():
    url = "http://fake_url.com/test_*_file.nc"  # wildcards

    with pytest.raises(FileNotFoundError):
        fd.core.get_url_list(url, use_cache=False)


def test_get_url_list_bad_filename_raise():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/bad_file_*_name.nc"  # wildcards
    )

    flist = fd.core.get_url_list(url, use_cache=False)
    assert flist == []


def test_get_url_list_fake_kwarg_https():
    url = (
        "http://dap.ceda.ac.uk/neodc/esacci"
        "/sea_surface_salinity/data/v02.31/7days/2012/01"
        "/ESACCI-SEASURFACESALINITY-L4-*_25km-*-fv2.31.nc"  # wildcards
    )

    with pytest.raises(KeyError):
        fd.core.get_url_list(url, use_cache=False, username="tester", password="fakes")


def test_choose_downloader():
    import pooch

    url = "ftp://thispartdoesntmatter.com"

    protocol = fd.core.choose_downloader(url, progress=False)

    assert protocol.__class__ == pooch.downloaders.FTPDownloader().__class__


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
    urls = fd.core.get_url_list(
        url, cache_path=f"{dest}/remote_files.cache", use_cache=True
    )[:1]
    fd.core.download_urls(urls, dest_dir=dest)


def test_make_readme():
    fname = "./tests/example_catalog.yml"

    cat = fd.read_catalog(fname)
    for key in cat:
        cat[key]["name"] = key.upper().replace("_", " ")
        fd.core.create_download_readme("README.txt", **cat[key])
