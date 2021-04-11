"""
Catalog
-------
Contains functions relating to reading in catalog files (YAML)
and ensuring that the entries are complete with metadata
"""


def read_catalog(catalog_name):
    """
    Used to read YAML files that contain download information.
    Placeholders for ENV names can also be used. See dotenv
    documentation for more info. The yaml files are structured
    as shown below

    .. code-block:: yaml

        url: remote path to file/s. Can contain *
        dest: path where the file/s will be stored (supports ~)
        meta: # meta will be written to README.txt
            doi: url to the data source
            description: info about the data
            citation: how to cite this dataset


    Args:
        catalog_name (str): the path to the catalog

    Returns:
        YAMLdict: a dictionary with catalog entries that is displayed
            as a YAML file. Can be viewed as a dictionary with the
            :code:`dict` method.

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

    catalog = YAMLdict(catalog)
    return catalog


class YAMLdict(dict):
    """
    A class that displays a dictionary in YAML format. The object is still a
    dictionary, it is just the representation that is displayed as a YAML
    dictionary. This makes it useful to create your own catalogs. You can
    use the method YAMLdict.dict to view the object in dictionary representation
    """

    def _strip_strings(self, d):
        """Removes extra lines and spaces from strings in a dictionary"""
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = self._strip_strings(v)
            elif isinstance(v, str):
                d[k] = v.strip()
            else:
                d[k] = v
        return d

    @property
    def dict(self):
        """returns a dictionary representation"""
        return dict(self)

    def _repr_html_(self):
        """returns YAML representation of a dictionary"""
        import yaml
        from pygments import highlight
        from pygments.lexers import get_lexer_by_name
        from pygments.formatters import HtmlFormatter
        import IPython

        dictionary = self._strip_strings(self)
        text = yaml.dump(self, default_flow_style=False, sort_keys=False)
        text = "\n".join([t[2:] for t in text.split("\n")[2:]])
        lexer = get_lexer_by_name("yaml")

        formatter = HtmlFormatter()
        output = IPython.display.HTML(
            '<style type="text/css">{style}</style>{content}'.format(
                style=formatter.get_style_defs(".highlight"),
                content=highlight(text, lexer, formatter),
            )
        ).data

        return output
