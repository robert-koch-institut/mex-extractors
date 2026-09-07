from typing import Any, cast

import yaml

from mex.common.assets import get_assets_connector


def read_bytes(path: str) -> bytes:
    """Read the contents of a file from the given path and return as bytes."""
    connector = get_assets_connector()
    return connector.read(path)


def load_yaml(path: str) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    raw_bytes = read_bytes(path)
    return cast("dict[str, Any]", yaml.safe_load(raw_bytes))


def glob_files(path: str, pattern: str) -> list[str]:
    """Glob files from the given path and return list of names.

    Args:
        path: path to files
        pattern: pattern to match

    Returns:
        list of file names
    """
    connector = get_assets_connector()
    return connector.glob(path, pattern)
