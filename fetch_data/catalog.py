"""
Contains functions relating to reading in catalog files (YAML)
and ensuring that the entries are complete with metadata
"""


def read_catalog(catalog_name):
    """
    Used to read YAML files that contain download information.
    Placeholders for ENV names can also be used. See dotenv
    documentation for more info.

    YAML file entries require:
        url: remote path to file/s. Can contain *
        dest: where the file/s will be stored
        meta:
            doi: url to the data source
            description: info about the data
            citation: how to cite this dataset

    Parameters
    ----------
    catalog_name: str
        the path to the catalog

    Returns
    -------
    dictionary with catalog entries
    """
    from dotenv import find_dotenv
    from envyaml import EnvYAML

    catalog_raw = EnvYAML(
        yaml_file=catalog_name, env_file=find_dotenv(), strict=True
    ).export()

    catalog = {}
    for k in catalog_raw:
        if ("." not in k) and (isinstance(catalog_raw[k], dict)):
            catalog[k] = catalog_raw[k]

    return catalog
