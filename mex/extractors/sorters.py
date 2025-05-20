from typing import TypeVar

from networkx import DiGraph
from networkx import topological_sort as network_sort

SortableT = TypeVar("SortableT")


def topological_sort(
    items: list[SortableT], primary_key: str, foreign_key: str
) -> list[SortableT]:
    """Sort the given items topologically."""
    graph = DiGraph()
    key_to_item: dict[str, SortableT] = {}
    for item in items:
        pk_value = getattr(item, primary_key)
        fk_value = getattr(item, foreign_key)
        key_to_item[pk_value] = item
        graph.add_node(pk_value)
        if fk_value:
            graph.add_edge(fk_value, pk_value)
    return [key_to_item[key] for key in network_sort(graph)]
