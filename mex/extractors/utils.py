from os import PathLike
from pathlib import Path
from typing import Any, TypeVar, cast

import yaml

T = TypeVar("T")


# TODO(ND): move to mex.common.utils
def ensure_list(values: list[T] | T | None) -> list[T]:
    """Wrap single objects in lists, replace None with [] and return lists untouched."""
    if values is None:
        return []
    if isinstance(values, list):
        return values
    return [values]


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    with Path(path).open(encoding="utf-8") as fh:
        return cast("dict[str, Any]", yaml.safe_load(fh))
