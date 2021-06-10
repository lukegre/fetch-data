"""
Utilities
---------
Helper functions for download. Only core python packages used in utils.
"""


def log_to_stdout(level=15):
    """
    Adds the stdout to the logging stream and sets the level to 15 by default
    """
    import logging

    logger = logging.getLogger("fetch_data")

    # remove existing file handlers
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.handlers.remove(handler)

    # add the new logger with the formatting
    logFormatter = logging.Formatter(
        "%(asctime)s [%(name)s]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(logFormatter)
    logger.addHandler(streamHandler)
    logger.setLevel(level)
    return logger


def log_to_file(fname):
    """
    Will append the given file path to the logger so that stdout
    and the file will be the output streams for the current logger
    """
    import logging
    from pathlib import Path as posixpath

    fname = posixpath(fname)
    fname.parent.mkdir(exist_ok=True, parents=True)

    logger = logging.getLogger("fetch_data")
    # remove existing file handlers
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            logger.handlers.remove(handler)

    # add the new logger with the formatting
    logFormatter = logging.Formatter(
        "%(asctime)s [%(name)s]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fileHandler = logging.FileHandler(fname)
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    logger.info("=" * 80)
    logger.info("Start of logging session")


def make_readme_file(dataset_name, url, meta={}, short_info_len_limit=150):
    """
    Adheres to the UP group's (ETHZ) readme prerequisites.

    Parameters
    ----------
    dataset_name: str
        The name of the dataset that will be at the top of the file
    url: str
        The url used to download the data - may be useful for other
        downloaders. May contain wildcards and placeholders.
    meta: dict
        A dictionary containing several

    """
    import inspect
    import os
    import pwd
    from textwrap import wrap
    from datetime import datetime

    assert isinstance(url, str)

    meta["data_preparation"] = (
        "Data has been downloaded directly from the server shown in URL. There has "
        "been no modification to the original files. There may be a data cache "
        "located in the destination folder.\n\n This README.txt file was "
        "automatically created while downloading data using the "
        "``fetch_data.download`` function. For more info see "
        "https://github.com/lukegre/fetch-data"
    )

    s = " " * 4
    w = "\n" + s

    line_limit = short_info_len_limit

    if len(dataset_name.strip()) > 2:
        line = "=" * len(dataset_name)
        dataset_name = dataset_name.replace("_", " ").replace("-", " ")
        name = s + w.join([line, dataset_name, line])
    else:
        name = ""

    # default inputs
    contact = meta.pop("contact", None)
    if contact is None:
        contact = get_git_username_and_email().get("email", None)
    if contact is None:
        contact = pwd.getpwuid(os.getuid())[0]

    today = datetime.today().strftime("%Y-%m-%d")

    # custom meta inputs (short)
    short = {k: v for k, v in meta.items() if len(k + v) <= line_limit}
    short_pretty = w.join([f"{k: <15s} {v}" for k, v in short.items()])

    # custom met inputs (long)
    long = {k: v for k, v in meta.items() if len(k + v) > line_limit}
    long_pretty = []
    for head, text in long.items():
        text = w.join(wrap(text.replace("\n", " "), 80))
        head = head.replace("_", " ").replace("-", " ").title()
        long_pretty += (w + w.join([f"{head}", f"{'-' * len(head)}", f"{text}"]) + w,)
    long_pretty = w.join(long_pretty).strip()

    try:
        from ._version import __version__ as version
    except ModuleNotFoundError:
        version = "no version found"

    # MAKING THE STRING
    readme_text = inspect.cleandoc(
        f"""{name}
    {'Contact': <15s} {contact}
    {'Date': <15s} {today}
    {'URL': <15s} {url}
    {'Script': <15s} https://github.com/lukegre/fetch-data ({version})
    {short_pretty}

    {long_pretty}

    """
    )
    meta.pop("data_preparation")
    return readme_text


def make_hash_string(string, output_length=10):
    """Create a hash for given string

    Truncates an md5 hash to the desired length.
    Will always be safe for file names.

    Args:
        string (str): input string
        output_length (int): length for output

    Returns:
        str: n character string that is unique to the input string
    """
    import base64
    import hashlib

    urlb = (string).encode()
    hasher = hashlib.md5(urlb).digest()
    hashed = base64.b16encode(hasher).decode()[:output_length]

    return hashed


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


def commong_substring(input_list):
    """Finds the common substring in a list of strings"""

    def longest_substring_finder(string1, string2):
        """Finds the common substring between two strings"""
        answer = ""
        len1, len2 = len(string1), len(string2)
        for i in range(len1):
            match = ""
            for j in range(len2):
                if i + j < len1 and string1[i + j] == string2[j]:
                    match += string2[j]
                else:
                    if len(match) > len(answer):
                        answer = match
                    match = ""
        return answer

    if len(input_list) == 2:
        return longest_substring_finder(*input_list)

    if len(input_list) > 2:
        item0 = input_list[0]
        for i in range(len(input_list) - 1):
            item1 = input_list[i + 1]
            item0 = commong_substring([item0, item1])
        return commong_substring([item0, item1])

    if len(input_list) == 1:
        return input_list[0]
