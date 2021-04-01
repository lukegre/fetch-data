"""
Helper functions for download

Only core python packages used in utils.
"""


def log_to_file(fname):
    """
    Will append the given file path to the logger so that stdout
    and the file will be the output streams for the current logger
    """
    import logging
    from pathlib import Path as posixpath

    fname = posixpath(fname)
    fname.parent.mkdir(exist_ok=True, parents=True)

    rootLogger = logging.getLogger()

    # remove existing file handlers
    for handler in rootLogger.handlers:
        if isinstance(handler, logging.FileHandler):
            rootLogger.handlers.remove(handler)

    # add the new logger with the formatting
    logFormatter = logging.Formatter(
        "%(asctime)s [DOWNLOAD]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fileHandler = logging.FileHandler(fname)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    logging.info("=" * 80 + "\n" * 2)
    logging.info("Start of logging session")
    logging.info("-" * 80)


def make_readme_file(
    dataset_name,
    doi_or_link,
    url,
    citation,
    description,
    variables,
    manipulation,
    download_logging="None",
    contact=None,
):
    """
    Adheres to the UP group's (ETHZ) readme prerequisites.

    Parameters
    ----------
    dataset_name: str
        The name of the dataset that will be at the top of the file
    doi_or_link: str
        The link to where the dataset can be downloaded or more info
        can be fetched
    url: str
        The url used to download the data - may be useful for other
        downloaders. May contain wildcards and placeholders.
    citation: str
        Just making it easier for people to cite the dataset. If there
        is more than one doi, please cite that too. This may be the case
        where a dataset is published alongside a paper.
    description: str
        The description of the data - can be copied directly from the website
        of the data.
    manipulation: str
        Any manipulation or changes you make to the data before saving.
    variables: list
        A list of the names of the variables that are downloaded
    download_loggin: str
        The path to the download logging. Defaults to None
    contact: str
        Defaults to Git email and then to USER if not provided.

    """
    import inspect
    import os
    import pwd
    from textwrap import wrap

    import pandas as pd

    if contact is None:
        contact = get_git_username_and_email().get("email", None)
    if contact is None:
        contact = pwd.getpwuid(os.getuid())[0] + " (USER)"

    today = pd.Timestamp.today().strftime("%Y-%m-%d")

    w = "\n" + " " * 4
    if variables == []:
        variables = ""
    elif isinstance(variables, list):
        variables = f"{w}Variables:{w}" + f"{w}".join(["- " + v for v in variables])
    else:
        variables = ""

    citation = w.join(wrap(citation.replace("\n", " "), 80))
    description = w.join(wrap(description.replace("\n", " "), 80))
    manipulation = w.join(wrap(manipulation.replace("\n", " "), 80))

    readme_text = inspect.cleandoc(
        f"""
    {'='*len(dataset_name)}
    {dataset_name}
    {'='*len(dataset_name)}

    Contact: {contact}
    Date:    {today}
    Source:  {doi_or_link}
    URL:     {url}
    Logging: {download_logging}


    ------------
    Dataset info
    ------------
    {citation}

    {description}
    {variables}

    ------------------
    Dataset processing
    ------------------
    {manipulation}



    readme file was automatically created using netCDFdownloader tool
    https://github.com/lukegre/netCDF-Downloader

    """
    )

    return readme_text


def flatten_list(list_of_lists):
    """Will recursively flatten a nested list"""
    if len(list_of_lists) == 0:
        return list_of_lists
    if isinstance(list_of_lists[0], list):
        return flatten_list(list_of_lists[0]) + flatten_list(list_of_lists[1:])
    return list_of_lists[:1] + flatten_list(list_of_lists[1:])


def get_kwargs():
    """
    Gets all the keyword, value pairings in the given function and
    returns them as a dictionary
    """
    import inspect

    frame = inspect.currentframe().f_back
    keys, _, _, values = inspect.getargvalues(frame)
    kwargs = {}
    for key in keys:
        if key != "self":
            kwargs[key] = values[key]
    return kwargs


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


def get_git_username_and_email():
    """will try to get the user and email from the git config"""
    import subprocess
    import re

    command = subprocess.run(["git", "config", "--list"], capture_output=True)
    config_str = command.stdout.decode()

    output = dict(re.findall("user.(name|email)=(.*)", config_str))
    return output
