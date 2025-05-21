from typing import TypeVar

from networkx import DiGraph
from networkx import topological_sort as network_sort

ItemT = TypeVar("ItemT")


def topological_sort(items: list[ItemT], primary_key: str, foreign_key: str) -> None:
    """Sort the given list of items in-place according to their topology.

    Items that reference an item's `primary_key` value, in a field specified by
    `foreign_key`, will be sorted lower in the list as the referenced item.

    This can be useful for submitting items to the backend in the correct order.
    """
    graph = DiGraph[str]()
    for item in items:
        pk_value = getattr(item, primary_key)
        graph.add_node(pk_value)
        if fk_value := getattr(item, foreign_key):
            graph.add_edge(fk_value, pk_value)
    sorted_keys = list(network_sort(graph))
    items.sort(key=lambda item: sorted_keys.index(getattr(item, primary_key)))
