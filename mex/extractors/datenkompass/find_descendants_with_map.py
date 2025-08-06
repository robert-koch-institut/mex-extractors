from typing import TypeVar

from mex.common.models import (
    ExtractedOrganizationalUnit,
    MergedOrganizationalUnit,
)

_OrganizationalUnit = TypeVar(
    "_OrganizationalUnit",
    MergedOrganizationalUnit,
    ExtractedOrganizationalUnit,
)


def find_descendants(items: list[_OrganizationalUnit], parent_id: str) -> list[str]:
    """Do something."""

    def build_child_map(items: list[_OrganizationalUnit]) -> dict[str, list[str]]:
        child_map: dict[str, list[str]] = {}
        for item in items:
            if item.parentUnit is not None:
                child_map.setdefault(str(item.parentUnit), []).append(
                    str(item.identifier)
                )
        return child_map

    def collect_descendants(
        child_map: dict[str, list[str]],
        node: str,
        descendants: set[str],
    ) -> None:
        for child_id in child_map.get(node, []):
            descendants.add(child_id)
            collect_descendants(child_map, child_id, descendants)

    child_map = build_child_map(items)

    descendants: set[str] = set()
    collect_descendants(child_map, str(parent_id), descendants)
    return list(descendants)
