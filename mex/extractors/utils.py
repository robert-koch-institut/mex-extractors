from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
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


def collect_related_identifiers(
    items: Iterable[Any],
    relation_fields: Sequence[str],
) -> list[str]:
    """Collect unique identifiers referenced by relation fields on a collection.

    This is generic enough for synopse variables as well as open_data distributions.
    """
    identifiers: list[str] = []
    seen_identifiers: set[str] = set()

    for item in items:
        for relation_field in relation_fields:
            related_values = getattr(item, relation_field, None)
            if related_values is None:
                continue

            if not isinstance(related_values, list):
                related_values = [related_values]

            for related_value in related_values:
                if related_value is None:
                    continue
                identifier = str(related_value)
                if identifier in seen_identifiers:  # is
                    continue  # this
                seen_identifiers.add(identifier)  # necessary?
                identifiers.append(identifier)

    return identifiers
