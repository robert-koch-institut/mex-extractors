from typing import Annotated, Final, Literal

from pydantic import Field

from mex.common.models import BaseModel

# TODO(ND): move these to mex-common


class AssetCheckRule(BaseModel):
    """Model to define asset checks against the count of an extractor asset."""

    fail_if: Literal[
        "less_than_x_inbound",
        "less_than_x_outbound",
        "not_exactly_x_items",
        "x_items_less_than",
        "x_items_more_than",
        "x_percent_less_than",
        "x_percent_more_than",
    ]
    value: int | float | None
    time_frame: Annotated[str, Field(pattern="^[0-9]{1,2}[dmy]$")] | None = None
    target_type: Literal[
        "Distribution",
        "Resource",
        "Variable",
        "VariableGroup",
        None,
    ] = None


class AssetCheck(BaseModel):
    """Model to bundle multiple asset check rules."""

    rules: list[AssetCheckRule]


CHECK_MODEL_CLASSES_BY_NAME: Final[dict[str, type[AssetCheck]]] = {
    "AssetCheck": AssetCheck
}
