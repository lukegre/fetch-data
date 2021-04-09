"""
Download
--------
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
    n_jobs=1,
    use_cache=True,
    cache_name="_urls_{hash}.cache",
    verbose=False,
    log_name="_downloads.log",
    raise_on_failed_url_fetch=True,
    decompress=True,
    create_readme=True,
    **kwargs,
):
    """Core function to fetch data from a url with a wildcard or as a list.

    Allows for parallel download of data that can be set with a single url containing
    a wild card character or a list of urls. If the wild card is used, file names
    will be cached. A README.txt file will automatically be generated in `dest`, along
    with a downloading log, and a url cachce (if url is a string).

    ``download`` is a Frankenstein mashup of ``fsspec`` and ``pooch`` to fetch files.
    It is tricky to download password protected files with ``fsspec`` and ``pooch``
    does not allow for wildcard listed downloads. If the url input is only a list,
    ``fsspec`` will not be used and only ``pooch``. But you can still download in
    parallel with this script.

    Args:
        url (str, list): if url is a list of urls, then urls will not be fetched
            and caching will not happen. Urls will be fetched directly.
            If url is a string with a wildcard (*), will try to search for more
            files. But this might not be possible with some HTTP websites.
        login (dict): required if username and passwords are required for protocol
        dest (str): where the files will be saved to. String formatting supported
            (as with url)
        n_jobs (int): the number of parallel downloads. Will not show progress bar
            when n_jobs > 1. Not allowed to be larger than 8.
        use_cache (bool): if set to True, will use cached url list instead of
            fetching a new list. This is useful for updating data
        cache_name (str): the file name to which data will be cached. This file is
            stored relative to ``dest``. The file is a simple text file showing a
            url for each line. This will not be used if a list is passed to url.
        verbose (bool / int): if verbose is False, logging level set to ERROR (40)
            if verbose is True, logging level set to 15
            if verbose is intiger, then sets logging level directly.
            See the logging module for more information.
        log_name (str): the file name to which logging will be saved. The file is stored
            relative to ``dest``. Logging level can be set with the ``verbose`` arg.
        kwargs (key=value): are keyword replacements for any values set in the
            url (if url is no a list) and dest strings

    Returns:
        list:
            a flattened list of file paths to where the data has been downloaded.
            If inputs are compressed, the names of the uncompressed files will be given.
    """
    from collections import defaultdict
    from .utils import get_kwargs, log_to_file, flatten_list
    from pathlib import Path as path

    # if any placeholders in dest, then fill them out
    dest = dest.format_map(kwargs)
    dest = str(path(dest).expanduser())

    # get all the inputs and store them as kwargs
    kwargs = {**get_kwargs(), **kwargs}

    # set logging level to 15 if verbose, else 40
    if isinstance(verbose, bool):
        logging_level = 15 if verbose else 40
    elif isinstance(verbose, int):
        logging_level = verbose
    else:
        raise TypeError("verbose must be bool or intiger")
    logging.getLogger().setLevel(logging_level)
    # Setting the logging file name and storing to kwargs for readme
    log_fname = f"{dest}/{log_name}"
    if logging_level < 40:
        log_to_file(log_fname)
        kwargs.update({"download_logging": log_fname})

    # creating the readme before downloading
    if create_readme:
        create_download_readme(**kwargs)

    # caching ignored if input is a list
    if isinstance(url, (list, tuple)):
        urls = url
        url = url[0]
    # fetches a list of the files in the directory
    elif "*" in url:
        urls = get_url_list(
            url=url.format_map(kwargs),
            raise_on_empty=raise_on_failed_url_fetch,
            use_cache=use_cache,
            cache_path=f"{dest}/{cache_name}",
            **login,
        )
    else:
        # will simply use url as is
        urls = [url.format_map(kwargs)]

    logging.log(20, f"{len(urls): >3} files at {url.format_map(kwargs)}")
    if len(urls) == 0:
        return []
    logging.log(20, f"Files will be saved to {dest}")

    flist = download_urls(
        urls,
        n_jobs=n_jobs,
        dest_dir=dest,
        login=login,
        decompress=decompress,
    )
    if flist is None:
        raise ValueError("Files could not be downloaded")

    return flatten_list(flist)


def get_url_list(
    url,
    username=None,
    password=None,
    use_cache=True,
    cache_path="./_urls_{hash}.cache",
    raise_on_empty=True,
    **kwargs,
):
    """If a url has a wildcard (*) value, remote files will be searched.

    Leverages off the `fsspec` package. This doesn't work for all HTTP urls.

    Parameters
    ----------
    url: str
        If a url has a wildcard (*) value, remote files will be searched for.
    username: str
        if required for given url and protocol (e.g. FTP)
    password: str
        if required for given url and protocol (e.g. FTP)
    cache_path: str
        the path where the cached files will be stored. Has a special case
        where `{hash}` will be replaced with a hash based on the URL.
    use_cache: bool
        if there is a file with cached remote urls, then those values will be
        returned as a list
    raise_on_empty: bool
        if there are no files, raise an error or silently pass

    Returns
    -------
    a sorted list of urls
    """
    from pathlib import Path as posixpath
    from urllib.parse import urlparse
    from .utils import make_hash_string

    if "*" not in url:
        return [url]

    if "{hash}" in cache_path:
        cache_path = cache_path.format(hash=make_hash_string(url))

    if use_cache:
        cache_path = posixpath(cache_path)
        if cache_path.is_file():
            with open(cache_path, "r") as file:
                flist = file.read().split("\n")
            logging.log(
                15, f"Fetched {len(flist)} files from flist cache: {cache_path}"
            )
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
        except Exception as e:
            if raise_on_empty:
                raise ValueError(f"URLs could not be fetched from {url}")
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
    dest_dir=".",
    login={},
    decompress=True,
    **kwargs,
):
    """
    Downloads the given list of urls to a specified destination path using
    the `pooch` package in Python.
    NOTE: `fsspec` is not used as it fails for some FTP and SFTP protocols.

    Args:
        urls (list): the list of URLS to download - may not contain wildcards
        dest_dir (str): the location where the files will be downloaded to. May contain
        date formatters that are labelled with "{t:%fmt} to create subfolders
        date_format (str): the format of the date in the urls that will be used to
        fill in the date formatters in `dest_dir` kwarg. Matches limited to
        1970s to 2020s
        kwargs (key=value): will be passed to pooch.retrieve. Can be used to set
        the downloader with username and password and the processor for unzipping.
        See `choose_downloader` for more info.

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

    from pooch import Unzip

    progressbar = (1 // n_jobs) & (logging.getLogger().level <= 20)
    download_args = []
    for url in urls:
        download_args += (
            dict(
                url=url,
                known_hash=None,
                fname=url.split("/")[-1],
                path=dest_dir,
                processor=choose_processor(url) if decompress else None,
                downloader=choose_downloader(url, login=login, progress=progressbar),
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


def choose_downloader(url, login={}, progress=True):
    """
    Will automatically select the correct downloader for the given url. Pass
    result to pooch.retrieve(downloader=downloader())

    Args:
        url (str): the path of a url

    Returns:
        pooch.Downloader: as a function. Resulting function Can be called with
        (username, password, progressbar) options.

    """
    from urllib.parse import urlparse as parse_url
    import pooch

    known_downloaders = {
        "ftp": pooch.FTPDownloader,
        "http": pooch.HTTPDownloader,
        "https": pooch.HTTPDownloader,
    }

    parsed_url = parse_url(url)
    if parsed_url.scheme not in known_downloaders:
        raise ValueError(
            f"Unrecognized URL protocol '{parsed_url.scheme}' in '{url}'. "
            f"Must be one of {known_downloaders.keys()}."
        )
    downloader = known_downloaders[parsed_url.scheme]

    # if http, then use different password implementation
    if url.lower().startswith("http") and (login != {}):
        login = dict(args=(login["username"], login["password"]))
    # calling the function to prepare
    downloader = downloader(progressbar=progress, **login)

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
            if ext in url.lower():
                chosen = processor
    return chosen


def create_download_readme(**entry):
    """
    Creates a README file based on the information in the source dictionary.

    Parameters
    ----------
    name: str
        name to which file will be written
    **entry: kwargs
        must contain
    """
    import inspect
    from pathlib import Path as posixpath
    from warnings import warn

    from .utils import make_readme_file

    dest = entry.get("dest")

    # readme will always be overwritten
    readme_fname = posixpath(f"{dest}/README.txt")
    print(readme_fname)
    readme_fname.parent.mkdir(parents=True, exist_ok=True)

    url = entry.get("url", None)
    if isinstance(url, (list, tuple)):
        url = url[0]

    readme_text = make_readme_file(
        entry.get("name", ""), url, entry.get("meta", {}), short_info_len_limit=len(url)
    )

    with open(readme_fname, "w") as file:
        file.write(readme_text)
