from os import PathLike
from typing import Any, cast

import yaml


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    with open(path, encoding="utf-8") as f:
        return cast(dict[str, Any], yaml.safe_load(f))
