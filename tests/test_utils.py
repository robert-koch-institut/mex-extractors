from datetime import date  # noqa: TC003

import pytest
from pydantic import ValidationError

from mex.common.models import BaseModel, ExtractedContactPoint, ExtractedResource
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.utils import (
    collect_related_identifier_counts,
    get_dtypes_for_model,
    model_copy_validated,
)


class DummyModel(BaseModel):
    bool_: bool
    str_: str
    date_: date
    float_: float
    int_: int


def test_get_dtypes_for_model() -> None:
    assert get_dtypes_for_model(DummyModel) == {
        "bool_": "bool",
        "str_": "string",
        "date_": "string",
        "float_": "Float64",
        "int_": "Int64",
    }


def test_collect_related_identifier_counts_groups_duplicate_references() -> None:
    identifier1 = MergedPrimarySourceIdentifier.generate(1)
    identifier2 = MergedPrimarySourceIdentifier.generate(2)
    identifier3 = MergedPrimarySourceIdentifier.generate(3)

    items = [
        ExtractedContactPoint(
            email=["resource@a.com"],
            hadPrimarySource=identifier1,
            identifierInPrimarySource="1",
        ),
        ExtractedContactPoint(
            email=["resource@a.com"],
            hadPrimarySource=identifier2,
            identifierInPrimarySource="2",
        ),
        ExtractedContactPoint(
            email=["resource@b.com", "resource@b.com"],
            hadPrimarySource=identifier3,
            identifierInPrimarySource="3",
        ),
    ]

    assert collect_related_identifier_counts(items, "email") == {
        items[0].stableTargetId: 1,
        items[1].stableTargetId: 1,
        items[2].stableTargetId: 2,
    }


def test_model_copy_validated() -> None:
    original = ExtractedResource(
        hadPrimarySource="00000000000000",
        identifierInPrimarySource="resource-1",
        accessRestriction="https://mex.rki.de/item/access-restriction-1",
        contact=["bFQoRhcVH5DHU6"],
        theme=["https://mex.rki.de/item/theme-11"],
        title=["Resource"],
        unitInCharge=["bFQoRhcVH5DHU8"],
    )

    copied = model_copy_validated(original, {"relatedResource": ["bFQoRhcVH5DHU9"]})

    assert copied is not original
    assert copied.relatedResource == ["bFQoRhcVH5DHU9"]
    assert original.relatedResource == []
    assert copied.identifier == original.identifier
    assert copied.stableTargetId == original.stableTargetId
    assert copied.model_dump(exclude={"relatedResource"}) == original.model_dump(
        exclude={"relatedResource"}
    )


def test_model_copy_validated_validates_update() -> None:
    original = ExtractedResource(
        hadPrimarySource="00000000000000",
        identifierInPrimarySource="resource-1",
        accessRestriction="https://mex.rki.de/item/access-restriction-1",
        contact=["bFQoRhcVH5DHU6"],
        theme=["https://mex.rki.de/item/theme-11"],
        title=["Resource"],
        unitInCharge=["bFQoRhcVH5DHU8"],
    )

    with pytest.raises(ValidationError, match="theme"):
        model_copy_validated(original, {"theme": []})
