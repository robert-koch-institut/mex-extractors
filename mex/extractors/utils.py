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
    for value in (variable.belongsTo, variable.usedIn):
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
    """Collect identifiers referenced by relation fields on a collection."""
    identifiers: list[str] = []

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
                identifiers.append(str(related_value))

    return identifiers


def collect_related_identifier_counts(
    items: Iterable[Any],
    relation_fields: Sequence[str],
) -> dict[str, int]:
    """Collect counts of identifiers referenced by relation fields on a collection."""
    identifier_counts: dict[str, int] = {}

    for identifier in collect_related_identifiers(items, relation_fields):
        identifier_counts[identifier] = identifier_counts.get(identifier, 0) + 1

    return identifier_counts
