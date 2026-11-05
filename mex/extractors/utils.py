from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable, Mapping

    from pandas._typing import Dtype
    from pydantic import BaseModel as PydanticBaseModel
    from typing_extensions import TypeForm

    from mex.common.models import AnyExtractedModel, BaseModel
    from mex.common.types import AnyMergedIdentifier


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
    items: Iterable[AnyExtractedModel],
    relation_field: str,
) -> dict[AnyMergedIdentifier, int]:
    """Collect counts of identifiers referenced by relation fields on a collection."""
    identifier_counts: dict[AnyMergedIdentifier, int] = {}

    for item in items:
        identifier_counts[item.stableTargetId] = len(getattr(item, relation_field, []))

    return identifier_counts


def model_copy_validated[ModelT: PydanticBaseModel](
    model: ModelT, update: Mapping[str, Any]
) -> ModelT:
    """Return a validated copy of a model with some fields updated.

    Unlike `model_copy`, this runs all field and model validators on the result and
    does not rely on assigning to the original, so it also works for frozen models.

    Args:
        model: Model instance to copy
        update: Values to change in the copy, keyed by field name

    Returns:
        New validated instance of the same model class
    """
    # TODO(ND): replace with `BaseModel.model_copy_validated` from future mex-common
    data = {name: getattr(model, name) for name in model.model_fields_set}
    return model.model_validate({**data, **update})
