from datetime import date  # noqa: TC003
from types import SimpleNamespace

from mex.common.models import BaseModel
from mex.extractors.utils import (
    collect_related_identifier_counts,
    collect_related_identifiers,
    get_dtypes_for_model,
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
