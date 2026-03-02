from typing import Protocol, runtime_checkable

from mex.common.models import AnyMergedModel


@runtime_checkable
class PublisherItemsLike(Protocol):
    """Structural PublisherItem type to conform to Dagster asset boundaries."""

    items: list[AnyMergedModel]
