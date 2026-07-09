from typing import TYPE_CHECKING, Protocol, runtime_checkable

from mex.common.models import AnyMergedModel, BaseModel

if TYPE_CHECKING:
    from mex.common.types import Year


@runtime_checkable
class PublisherItemsLike(Protocol):
    """Structural PublisherItem type to conform to Dagster asset boundaries."""

    items: list[AnyMergedModel]


class BibliographicResourceForCsv(BaseModel):
    """pydantic model of Bibliographic Resources for writing to csv."""

    contributingUnit: list[str] | None
    publicationYear: Year | None
    creator: list[str]
    title: list[str]
    journal: list[str] | None
    doi: str | None
    accessRestriction: str
    publisher: list[str] | None
