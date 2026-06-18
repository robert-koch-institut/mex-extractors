from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml

from mex.common.backend_api.connector import BackendApiConnector

if TYPE_CHECKING:
    from os import PathLike

    from mex.common.models import ExtractedVariable


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    with Path(path).open(encoding="utf-8") as fh:
        return cast("dict[str, Any]", yaml.safe_load(fh))


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


def count_inbound_connections(list_of_ids: list[str], target_type: str) -> int:
    """Count the number of inbound connections for a given list of identifiers."""
    # graph query to all target types, checking separately for each identifier
    # if it is referenced, then add all counts together and return the total count
    total_count = 0
    connector = BackendApiConnector()
    for identifier in list_of_ids:
        result = connector.fetch_extracted_items(
            entity_type=[target_type],
            referenced_identifier=[identifier],
        )
        count_for_id = len(result.items)
        total_count += count_for_id

    return total_count
