from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml

if TYPE_CHECKING:
    from os import PathLike


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    with Path(path).open(encoding="utf-8") as fh:
        return cast("dict[str, Any]", yaml.safe_load(fh))
