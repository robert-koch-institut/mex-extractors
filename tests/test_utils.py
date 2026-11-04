from datetime import date  # noqa: TC003
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from mex.common.models import BaseModel, ExtractedResource
from mex.common.testing import Joker
from mex.extractors.utils import (
    collect_related_identifier_counts,
    collect_related_identifiers,
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


def test_collect_related_identifiers_keeps_duplicate_references() -> None:
    items = [
        SimpleNamespace(usedIn="resource-a"),
        SimpleNamespace(usedIn="resource-a"),
        SimpleNamespace(usedIn=["resource-b", None, "resource-b"]),
    ]

    assert collect_related_identifiers(items, ["usedIn"]) == [
        "resource-a",
        "resource-a",
        "resource-b",
        "resource-b",
    ]


def test_collect_related_identifier_counts_groups_duplicate_references() -> None:
    items = [
        SimpleNamespace(usedIn="resource-a"),
        SimpleNamespace(usedIn="resource-a"),
        SimpleNamespace(usedIn=["resource-b", None, "resource-b"]),
    ]

    assert collect_related_identifier_counts(items, ["usedIn"]) == {
        "resource-a": 2,
        "resource-b": 2,
    }


def test_model_copy_validated() -> None:
    original = ExtractedResource(
        hadPrimarySource="00000000000000",
        identifierInPrimarySource="resource-1",
        accessRestriction="https://mex.rki.de/item/access-restriction-1",
        contact=["bFQoRhcVH5DHU6"],
        resourceCreationMethod=Joker(),
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
        resourceCreationMethod=Joker(),
        theme=["https://mex.rki.de/item/theme-11"],
        title=["Resource"],
        unitInCharge=["bFQoRhcVH5DHU8"],
    )

    with pytest.raises(ValidationError, match="theme"):
        model_copy_validated(original, {"theme": []})
