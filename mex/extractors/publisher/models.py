from datetime import date  # noqa: TC003
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


class CsvResource(BaseModel):
    """pydantic model of CSV resources listed in datapackage.json."""

    name: str
    title: str
    type: str = "table"
    path: str
    scheme: str = "file"
    format: str = "csv"
    mediatype: str = "text/csv"
    encoding: str = "utf-8"


class CsvDataPackage(BaseModel):
    """pydantic model of datapacke content."""

    name: str = "rki-mex-csv-publication-reports"
    title: str = "RKI Publikationslisten"
    created: date
    resources: list[CsvResource]
