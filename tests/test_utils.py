from types import SimpleNamespace

from mex.extractors.utils import collect_related_identifiers


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
