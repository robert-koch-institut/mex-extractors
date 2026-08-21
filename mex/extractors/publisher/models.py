from typing import Protocol, runtime_checkable

from mex.common.models import AnyMergedModel, BaseModel


@runtime_checkable
class PublisherItemsLike(Protocol):
    """Structural PublisherItem type to conform to Dagster asset boundaries."""

    items: list[AnyMergedModel]


class BibliographicResourceForCsv(BaseModel):
    """pydantic model of Bibliographic Resources for writing to csv."""

    contributingUnit: list[str] | None
    publicationYear: str | None
    creator: list[str]
    title: list[str]
    journal: list[str] | None
    doi: str | None
    accessRestriction: str
    publisher: list[str] | None
