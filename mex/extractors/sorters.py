from typing import TypeVar

from networkx import DiGraph
from networkx import topological_sort as network_sort

ItemT = TypeVar("ItemT")


def topological_sort(
    items: list[ItemT],
    primary_key: str,
    *,
    parent_key: str | None = None,
    child_key: str | None = None,
) -> None:
    """Sort the given list of items in-place according to their topology.

    Items can reference other item's `primary_key` value in a field specified by
    `parent_key` pointing towards a parent item can reference a child item using
    the `child_key` field.

    This can be useful for submitting items to the backend in the correct order.
    """

    def ensure_list(value: list[str] | str | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    graph: DiGraph[str] = DiGraph()
    for item in items:
        current_node = getattr(item, primary_key)
        if parent_key:
            for parent_node in ensure_list(getattr(item, parent_key)):
                graph.add_edge(parent_node, current_node)
        if child_key:
            for child_node in ensure_list(getattr(item, child_key)):
                graph.add_edge(current_node, child_node)

    sorted_keys = list(network_sort(graph))
    items.sort(
        key=lambda item: (
            sorted_keys.index(getattr(item, primary_key)),
            getattr(item, primary_key),
        )
    )
