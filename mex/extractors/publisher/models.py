from typing import Protocol, runtime_checkable

from pydantic import Field

from mex.common.models import AnyMergedModel, BaseModel


@runtime_checkable
class PublisherItemsLike(Protocol):
    """Structural PublisherItem type to conform to Dagster asset boundaries."""

    items: list[AnyMergedModel]


class BibliographicResourceForCsv(BaseModel):
    """pydantic model of Bibliographic Resources for writing to csv."""

    contributingUnit: list[str] | None = Field(
        None, serialization_alias="Mitwirkende Fachgebiete"
    )
    publicationYear: str | None = Field(
        None, serialization_alias="Veröffentlichungsjahr"
    )
    creator: list[str] = Field(..., serialization_alias="Autor*innen")
    title: list[str] = Field(..., serialization_alias="Titel")
    journal: list[str] | None = Field(None, serialization_alias="Zeitschrift")
    doi: str | None = Field(None, serialization_alias="DOI")
    accessRestriction: str = Field(..., serialization_alias="Zugriffsbeschränkung")
    publisher: list[str] | None = Field(None, serialization_alias="Verlag")
