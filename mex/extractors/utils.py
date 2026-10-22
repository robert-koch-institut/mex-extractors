from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from pandas._typing import Dtype
    from typing_extensions import TypeForm

    from mex.common.models import BaseModel


PANDAS_DTYPE_MAP: defaultdict[TypeForm[Any], str] = defaultdict(
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
        f.alias or name: PANDAS_DTYPE_MAP[
            f.annotation if f.annotation is not None else type(None)
        ]
        for name, f in model.model_fields.items()
    }


def collect_related_identifier_counts(
    items: Iterable[Any],
    relation_field: str,
) -> dict[str, int]:
    """Collect counts of identifiers referenced by relation fields on a collection."""
    identifier_counts: dict[str, int] = {}

    for item in items:
        identifier_counts[getattr(item, "stableTargetId")] = len(  # noqa: B009
            getattr(item, relation_field, [])
        )

    return identifier_counts
