import logging
import warnings

import fsspec
import pooch

warnings.filterwarnings("ignore", category=RuntimeWarning)


def get_cache_path(url, cache_dir=None):
    """
    Creates the path for the cache used to store remote file names
    Saves time in updating the
    """
    import hashlib
    import tempfile
    from pathlib import Path as posixpath

    if cache_dir is None:
        cache_dir = tempfile.gettempdir()

    cache_fname = hashlib.md5(str(url).encode()).hexdigest()
    cache_path = posixpath(f"{cache_dir}/{cache_fname}")

    return cache_path


def abbreviate_list_as_str(ls):
    """
    Abbreviates a list when it's too long to show everything

    Used mostly in logging.DEBUG
    """
    n = len(ls)
    if n > 4:
        return f"{str(ls[:2])[:-1]},\n...\n{str(ls[-2:])[1:]}"
    else:
        return f"{str(ls)}"


def get_url_list(
    url,
    username=None,
    password=None,
    cache_path=None,
    use_cache=True,
    raise_on_empty=True,
):
    """
    If a url has a wildcard (*) value, remote files will be searched for.
    Leverages off the `fsspec` package. This doesnt work for all HTTP urls.

    Parameters
    ----------
    username: str
        if required for given url and protocol (e.g. FTP)
    password: str
        if required for given url and protocol (e.g. FTP)
    cache_path: str
        the path where the cached files will be stored
    use_cache: bool
        if there is a file with cached remote urls, then those
        values will be returned as a list
    raise_on_empty: bool
        if there are no files, raise an error or silently pass

    Returns
    -------
    a sorted list of urls
    """
    from pathlib import Path as posixpath
    from urllib.parse import urlparse

    from aiohttp import ClientResponseError
    from pandas import Series, read_csv

    if cache_path is None:
        cache_path = get_cache_path(url)
    else:
        cache_path = posixpath(cache_path)

    if cache_path.is_file() and use_cache:
        flist = read_csv(str(cache_path), index_col=False).iloc[:, 0].to_list()
        logging.log(15, f"Fetched {len(flist)} files from flist cache: {cache_path}")
        logging.debug(abbreviate_list_as_str(flist))

        return sorted(flist)

    purl = urlparse(url)
    protocol = purl.scheme
    host = purl.netloc
    path = purl.path

    logging.log(15, f"Fetching filenames from {url}")

    props = {"protocol": protocol}
    if not protocol.startswith("http"):
        props.update({"host": host})
    if username is not None:
        props["username"] = username
    if password is not None:
        props["password"] = password

    fs = fsspec.filesystem(**props)
    if protocol.startswith("http"):
        path = f"{protocol}://{host}/{path}"
        try:
            flist = fs.glob(path)
        except ClientResponseError:
            if raise_on_empty:
                raise ValueError(f"No files could be found for the url: {url}")
            else:
                return []
    else:
        flist = [f"{protocol}://{host}{f}" for f in fs.glob(path)]

    no_files = len(flist) == 0
    if no_files and raise_on_empty:
        raise ValueError(f"No files could be found for the url: {url}")

    if no_files and not use_cache:
        return flist

    cache_path.parent.mkdir(exist_ok=True, parents=True)
    # writing url list to cache file
    Series(flist, dtype="str").to_csv(str(cache_path), index=False)
    logging.log(15, f"Cached {len(flist)} urls to: {cache_path}")
    logging.debug(abbreviate_list_as_str(flist))

    return sorted(flist)


def download_urls(urls, dest_path="./{t:%Y}/{t:%m}", date_format="%Y%m%d", **kwargs):
    """
    Downloads the given list of urls to a specified destination path using
    the `pooch` package in Python.

    NOTE: `fsspec` is not used as it fails for some FTP and SFTP protocols.

    Parameters
    ----------
    urls: list
        the list of URLS to download - may not contain wildcards
    dest_path: str
        the location where the files will be downloaded to. May contain
        date formatters that are labelled with "{t:%fmt} to create subfolders
    date_format: str
        the format of the date in the urls that will be used to fill in the
        date formatters in `dest_path` kwarg. Matches limited to 1970s to 2020s
    kwargs: key=value
        will be passed to pooch.retrieve. Can be used to set the downloader
        with username and password and the processor for unzipping. See
        `choose_downloader` for more info.

    Returns
    -------
    file names of downloaded urls
    """
    import pandas as pd
    from datetime_matcher import DatetimeMatcher

    re_date = DatetimeMatcher()
    # format will limit between 1970s and 2020s (with exceptions)
    re_date.format_code_to_regex_map["Y"] = "[12][90][789012][0-9]"

    flist = []
    for url in urls:
        if "{t:" in dest_path:
            date = re_date.extract_datetime(date_format, url)
            if date is not None:
                date = pd.to_datetime(date, format=date_format)
                fpath = dest_path.format(t=date)
            else:
                raise ValueError("No date found in the dest_path")
        else:
            fpath = dest_path

        split_url = url.split("/")
        flist += (
            pooch.retrieve(
                url=url, known_hash=None, fname=split_url[-1], path=fpath, **kwargs
            ),
        )

    return sorted(flist)


def choose_downloader(url):
    """
    Will automatically select the correct downloader for the given url.
    Pass result to pooch.retrieve(downloader=downloader(**kwargs))

    Parameters
    ----------
    url: str
        the path of a url

    Returns
    -------
    pooch.Downloader as a function. Resulting function Can be called with
    (username, password, progressbar) options.
    """
    from urllib.parse import urlparse as parse_url

    import pooch

    known_downloaders = {
        "ftp": pooch.FTPDownloader,
        "https": pooch.HTTPDownloader,
        "http": pooch.HTTPDownloader,
    }

    parsed_url = parse_url(url)
    if parsed_url.scheme not in known_downloaders:
        raise ValueError(
            f"Unrecognized URL protocol '{parsed_url.scheme}' in '{url}'. "
            f"Must be one of {known_downloaders.keys()}."
        )
    downloader = known_downloaders[parsed_url.scheme]

    return downloader


def shorten_url(s, len_limit=75):
    """
    Make url shorter with max len set to len_limit
    """

    if len(s) > len_limit:
        split = s.split("/")
    else:
        return s

    short = split[0]
    for s in split[1:-1]:
        if (len(short + split[-1]) + 5) > len_limit:
            short += "/.../" + split[-1]
            return short
        else:
            short += "/" + s
    return short


def retrieve_files(
    year=2012,
    url="",
    dest="./",
    use_cache=True,
    login={},
    is_era5=False,
    cds_var_names=None,
    name="",
    **kwargs,
):
    """
    A high level function to retrieve data from a url with a wildcard.
    Limited to fetching one year at a time.

    TODO: make url string formatting more versatile

    Parameters
    ----------
    year: int
        the year to fetch data for
    url: str
        a url with wildcards (*) and {year} formatter
        additional formatters will later be an option
    dest: str
        where the files will be saved to. string formatters
        will also be replaced (at the moment only supports {year})
    use_cache: bool
        if set to True, will use cached url list instead of fetching a
        new list. This is useful for updating data
    login: dict
        required if username and passwords are required for protocol
    is_era5: bool
        will use the era5 protocol if set to True.
    cds_var_names: list
        only if is_era = True, will fetch the given files.
    name: str
        used to keep track in logging. can set this to the data source
    **kwargs:
        not used.
    """

    import ftplib
    from collections import defaultdict

    from pooch import Unzip

    from .utils import get_kwargs, log_to_file

    log_to_file(f"{dest}/logging_downloads.log".format(year=""))

    if is_era5:
        return era5_data(year=year, cds_var_names=cds_var_names, dest=dest, **kwargs)

    sources = get_kwargs()
    create_download_readme(name, **sources)

    replacements = defaultdict(str)
    replacements.update({"year": year})
    dest = dest.format_map(replacements)

    urls = get_url_list(
        url=url.format_map(replacements),
        raise_on_empty=False,
        cache_path=f"{dest}/.flist_{year}.chache",
        use_cache=use_cache,
        **login,
    )

    url_short = shorten_url(url.format(year=year))
    logging.info(
        f"[DOWNLOAD] {year}: {name.upper(): <15s} {len(urls): >3} " f"files {url_short}"
    )

    logging.log(15, f"{len(urls)} files exist for {url}".format(year=year))

    if len(urls) == 0:
        return []

    downloader = choose_downloader(url)
    processor = Unzip() if urls[0].endswith(".zip") else None

    flist = None
    try:
        flist = download_urls(
            urls,
            dest_path=dest,
            downloader=downloader(progressbar=True, **login),
            processor=processor,
        )
    except ftplib.error_perm:
        flist = download_urls(
            urls,
            dest_path=dest,
            downloader=downloader(progressbar=False, **login),
            processor=processor,
        )
    if flist is None:
        raise ValueError("Files could not be downloaded")

    return flist


def era5_data(
    year=2020,
    month=None,
    day=None,
    dest="./",
    cds_var_names=[
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "mean_sea_level_pressure",
    ],
    **kwargs,
):
    """
    Shortcut for fetching era5 data. requires the `cdsapi` to be
    correctly set up (~/.cdsapi)

    Uses data from `reanalysis-era5-single-levels`
    Fetches data as monthly files to keep requests to a reasonable size

    """
    from pathlib import Path as posixpath

    import cdsapi
    from joblib import Parallel, delayed
    from numpy import ndarray

    cds_client = cdsapi.Client()

    assert isinstance(year, (int, float, str))
    assert isinstance(cds_var_names, (str, list, tuple))

    year = str(year)
    month = [f"{m:02d}" for m in range(1, 13)] if month is None else month
    day = [f"{d:02d}" for d in range(1, 32)] if day is None else day

    logging.info(
        f'[DOWNLOAD] {year}: {"ERA5_DATA": <15s}  12 '
        f"files https://cds.climate.copernicus.eu/cdsapp#!/yourrequests"
    )
    if isinstance(month, (tuple, list, ndarray)):
        inputs = []
        for m in month:
            inputs += (
                dict(
                    year=year,
                    month=m,
                    day=day,
                    dest=dest,
                    cds_var_names=cds_var_names,
                    **kwargs,
                ),
            )
        results = Parallel(n_jobs=12)(
            delayed(era5_data)(**input_dict) for input_dict in inputs
        )

        return results
    # If months are not iterable, then run a request and download monthly data
    else:
        extra = ""
        extra += f"{month}" if isinstance(month, str) else ""
        extra += f"{day}" if isinstance(day, str) else ""

        sname = dest.format(year=year, month=month, day=day)
        ppath = posixpath(sname)
        print(ppath)
        if ppath.is_file():
            return sname

        ppath.parent.mkdir(exist_ok=True, parents=True)

        cds_client.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": cds_var_names,
                "year": year,
                "month": month,
                "day": day,
                "time": [
                    "00:00",
                    "01:00",
                    "02:00",
                    "03:00",
                    "04:00",
                    "05:00",
                    "06:00",
                    "07:00",
                    "08:00",
                    "09:00",
                    "10:00",
                    "11:00",
                    "12:00",
                    "13:00",
                    "14:00",
                    "15:00",
                    "16:00",
                    "17:00",
                    "18:00",
                    "19:00",
                    "20:00",
                    "21:00",
                    "22:00",
                    "23:00",
                ],
            },
            sname,
        )

        return sname


def update_year(year, sources_dict, use_cache=True):
    flists = {}
    for key in sources_dict:
        flists[key] = retrieve_files(
            year=year, name=key, use_cache=use_cache, **sources_dict[key]
        )

    return flists


def create_download_readme(name, **source_dict):
    import inspect
    from pathlib import Path as posixpath
    from warnings import warn

    from .utils import make_readme_file

    dest = source_dict.get("dest").format(year="")
    cache_fname = f"{source_dict.get('dest')}/.flist_{{year}}.chache"
    manipulation = inspect.cleandoc(
        f"""
    Data has been downloaded directly from the server shown in URL.
    There has been no modification to the original files.
    There may be a data cache located in the annual subfolders of each
    with the format {cache_fname.replace('//', '/')}
    """
    )

    args = [
        name,
        source_dict.get("meta", {}).get("doi", None),
        source_dict.get("url", None),
        source_dict.get("meta", {}).get("citation", None),
        source_dict.get("meta", {}).get("description", None),
        source_dict.get("variables", []),
        manipulation,
    ]

    readme_fname = posixpath(f"{dest}/readme.txt")
    if readme_fname.is_file():
        warn(f"the file {readme_fname} already exists. " "Will not be overwritten. ")
        return

    readme_fname.parent.mkdir(parents=True, exist_ok=True)

    email = source_dict.get("email", None)
    logging = source_dict.get("download_logging", "None")

    readme_text = make_readme_file(*args, email=email, download_logging=logging)

    with open(readme_fname, "w") as file:
        file.write(readme_text)
