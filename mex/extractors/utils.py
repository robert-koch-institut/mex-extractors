from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Sequence

    from pandas._typing import Dtype

    from mex.common.models import BaseModel


PANDAS_DTYPE_MAP = defaultdict(
    lambda: "string",
    {bool: "bool", float: "Float64", int: "Int64"},
)


def get_dtypes_for_model(model: type[BaseModel]) -> dict[str, Dtype]:
    """Get the basic dtypes per field for a model from the `PANDAS_DTYPE_MAP`.

    Args:
        model: Model class for which to get pandas data types per field alias

    Returns:
        Mapping from field alias to dtype strings
    """
    return {
        f.alias or name: PANDAS_DTYPE_MAP[f.annotation or type(None)]
        for name, f in model.model_fields.items()
    }


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
