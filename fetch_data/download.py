"""
Module to download files on remote servers.

download is the primary interface, with all other functions supporting.
"""

import logging
import tempfile
import warnings

import fsspec
import pooch


warnings.filterwarnings("ignore", category=RuntimeWarning)


def download(
    url="",
    login={},
    dest="./",
    n_jobs=8,
    use_cache=True,
    verbose=True,
    cache_name="FD_remote_files.cache",
    log_name="FD_downloads.log",
    **kwargs,
):
    """
    A high level function to retrieve data from a url with a wildcard.

    Parameters
    ----------
    url: str
        a url with wildcards (*) formatter. Python string formatters that
        match kwarg entries will be replaced.
    dest: str
        where the files will be saved to. String formatting supported (as with url)
    use_cache: bool
        if set to True, will use cached url list instead of fetching a
        new list. This is useful for updating data
    n_jobs: int
        the number of parallel downloads. Will not show progress bar
        when n_jobs > 1
    login: dict
        required if username and passwords are required for protocol
    name: str
        used to keep track in logging. can set this to the data source
    verbose: bool / int
        if verbose is False, logging level set to ERROR (40)
        if verbose is True, logging level set to 15
        if verbose is intiger, then sets logging level directly.
        See the logging module for more information.
    **kwargs:
        not used.
    """
    from collections import defaultdict

    # get all the inputs and store them as kwargs
    kwargs = {**get_kwargs(), **kwargs}
    # if any placeholders in dest, then fill them out
    dest = dest.format_map(kwargs)

    # set logging level to 15 if verbose, else 40
    if isinstance(verbose, bool):
        logging_level = 15 if verbose else 40
    elif isinstance(verbose, int):
        logging_level = verbose
    else:
        raise TypeError("verbose must be bool or intiger")
    logging.getLogger().setLevel(logging_level)
    # Setting the logging file name and storing to kwargs for readme
    if logging_level < 40:
        log_fname = f"{dest}/{log_name}"
        log_to_file(log_fname)
    kwargs.update({"download_logging": log_fname})

    # creating the readme before downloading
    create_download_readme(**kwargs)

    # fetches a list of the files in the directory
    urls = get_url_list(
        url=url.format_map(kwargs),
        raise_on_empty=False,
        use_cache=use_cache,
        cache_path=f"{dest}/{cache_name}",
        **login,
    )

    logging.log(20, f"{len(urls): >3} files at {url.format_map(kwargs)}")
    logging.log(20, f"Files will be saved to {dest}")

    if len(urls) == 0:
        return []

    # determine HTTP, FTP, SFTP. Log in details are consistent of all files
    downloader = choose_downloader(url)
    flist = download_urls(
        urls,
        n_jobs=n_jobs,
        dest_path=dest,
        downloader=downloader(progressbar=True, **login),
    )
    if flist is None:
        raise ValueError("Files could not be downloaded")

    return flatten_list(flist)


def get_url_list(
    url,
    username=None,
    password=None,
    use_cache=True,
    cache_path=None,
    raise_on_empty=True,
):
    """
    If a url has a wildcard (*) value, remote files will be searched for.
    Leverages off the `fsspec` package. This doesn't work for all HTTP urls.

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

    if use_cache:
        assert isinstance(cache_path, str), "cache_path must be a string"
        cache_path = posixpath(cache_path)
        if cache_path.is_file():
            with open(cache_path, "r") as file:
                flist = file.read().split("\n")
            logging.log(
                15, f"Fetched {len(flist)} files from flist cache: {cache_path}"
            )
            logging.debug(flist)

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

    # writing url list to cache file
    if use_cache:
        cache_path.parent.mkdir(exist_ok=True, parents=True)
        with open(cache_path, "w") as out_file:
            out_file.write("\n".join(flist))

    logging.log(15, f"Cached {len(flist)} urls to: {cache_path}")
    logging.debug(flist)

    return sorted(flist)


def download_urls(
    urls,
    downloader=None,
    n_jobs=8,
    dest_path="./{t:%Y}/{t:%m}",
    date_format="%Y%m%d",
    **kwargs,
):
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

    def pooch_retrieve_handling(kwargs):
        """
        An internal function to process errors and avoid failed downloads:
        - using the progressbar is not allowed by the server
        - will detect if permissions are not sufficient for downloading

        Parameters
        ----------
        kwargs: dict
            a dictionary containing all the info required to download data

        Returns
        -------
        int:
            0 = success
            1 = failure
        str:
            retrieved filename, if failed returns the URL
        """
        pooch.get_logger().setLevel(1000)
        url = kwargs.get("url")

        try:
            logging.log(15, f"retrieving {url}")
            return 0, pooch.retrieve(**kwargs)
        except:
            pass

        try:
            # this is for when the server does not allow the file size to be fetched
            kwargs["downloader"].progressbar = False
            return 0, pooch.retrieve(**kwargs)
        except:
            pass

        # this will raise the error
        try:
            pooch.retrieve(**kwargs)
        except Exception as e:
            if "550" in str(e):
                message = f"ERROR: Check file permissions: {url}. "
                logging.log(20, message)
            return 1, url
        finally:
            return 1, url

    import pandas as pd
    from datetime_matcher import DatetimeMatcher
    from pooch import Unzip

    re_date = DatetimeMatcher()
    # format will limit between 1970s and 2020s (with exceptions)
    re_date.format_code_to_regex_map["Y"] = "[12][90][789012][0-9]"

    download_args = []
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

        download_args += (
            dict(
                url=url,
                known_hash=None,
                fname=url.split("/")[-1],
                path=fpath,
                processor=choose_processor(url),
                downloader=downloader,
                **kwargs,
            ),
        )

    # n_jobs will default to number of urls if less than given n_jobs
    n_jobs = min([n_jobs, len(download_args)])
    if n_jobs == 1:  # will not use joblib if n_jobs=1
        flist = [pooch_retrieve_handling(d) for d in download_args]
    elif 1 < n_jobs <= 8:  # uses joblib for parallel downloads
        from joblib import Parallel, delayed

        flist = Parallel(n_jobs=n_jobs, prefer="threads")(
            delayed(pooch_retrieve_handling)(d) for d in download_args
        )
    else:  # max set to 8 for safety - sometimes too many connections
        raise Exception("n_jobs must be between 1 and 8 to avoid too many requests")

    failed = [f for o, f in flist if o > 0]
    passed = [f for o, f in flist if o == 0]
    logging.info(
        f"SUMMARY: Retrieved={len(passed)}, Failed={len(failed)} listing failed below: \n"
        + "\n".join(failed)
    )

    return passed


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


def choose_processor(url):
    """
    chooses the processor to uncompress if required
    """
    known_processors = {
        pooch.Decompress(): (".gz2", ".gz"),
        pooch.Untar(): (".tar", ".tgz", ".tar.gz"),
        pooch.Unzip(): (".zip",),
        None: "*",
    }

    chosen = None
    for processor, extensions in known_processors.items():
        for ext in extensions:
            if ext in url:
                chosen = processor
    return chosen


def create_download_readme(name, **source_dict):
    """
    Creates a README file based on the information in the source dictionary.

    Parameters
    ----------
    name: str
        name to which file will be written
    **source_dict: kwargs
        must contain
    """
    import inspect
    from pathlib import Path as posixpath
    from warnings import warn

    from .utils import make_readme_file

    dest = source_dict.get("dest")
    manipulation = inspect.cleandoc(
        f"""
    Data has been downloaded directly from the server shown in URL.
    There has been no modification to the original files.
    There may be a data cache located in the destination folder
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

    # readme will always be overwritten
    readme_fname = posixpath(f"{dest}/readme.txt")
    readme_fname.parent.mkdir(parents=True, exist_ok=True)

    contact = source_dict.get("contact", None)
    logging = source_dict.get("download_logging", "None")

    readme_text = make_readme_file(*args, contact=contact, download_logging=logging)

    with open(readme_fname, "w") as file:
        file.write(readme_text)
