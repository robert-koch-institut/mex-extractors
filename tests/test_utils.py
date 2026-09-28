from datetime import date  # noqa: TC003
from types import SimpleNamespace

from mex.common.models import BaseModel
from mex.common.types import AnonymizationPseudonymization, MIMEType
from mex.extractors.utils import (
    collect_related_identifier_counts,
    collect_related_identifiers,
    find_vocabulary_member,
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


def test_find_vocabulary_member_matches_pref_label() -> None:
    assert (
        find_vocabulary_member(AnonymizationPseudonymization, "pseudonymisiert")
        == AnonymizationPseudonymization["PSEUDONYMIZED"]
    )


def test_find_vocabulary_member_matches_alt_label() -> None:
    assert find_vocabulary_member(MIMEType, "Word document") == MIMEType["DOCX"]


def test_find_vocabulary_member_returns_none_for_no_match() -> None:
    assert find_vocabulary_member(MIMEType, "not-a-real-label") is None


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
