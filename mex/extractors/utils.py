from typing import TYPE_CHECKING, Any, cast

import yaml

if TYPE_CHECKING:
    from os import PathLike

    from mex.common.models import ExtractedVariable


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    from mex.common.assets import get_assets_connector
    file_contents = get_assets_connector().load_file(path)
    return cast("dict[str, Any]", yaml.safe_load(file_contents.decode('utf-8')))


def count_outbound_connections(variable: ExtractedVariable) -> int:
    """Count the number of outbound connections for a given ExtractedVariable."""
    count = 0
    for value in (variable.belongsTo, variable.usedIn, variable.hadPrimarySource):
        if value is None:
            continue
        if isinstance(value, list):
            count += sum(item is not None for item in value)
        else:
            count += 1
    return count
