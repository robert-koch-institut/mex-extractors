from pydantic import BaseModel


class OpenDataCreatorsOrContributors(BaseModel):
    """Model subclass for Zenodo metadata Creators or Contributors."""

    name: str | None = None


class OpenDataRelateditdentifiers(BaseModel):
    """Model subclass for Zenodo metadata related identifiers."""

    identifier: str | None = None
    relation: str | None = None


class OpenDataLicense(BaseModel):
    """Model subclass for Zenodo metadata license."""

    id: str | None = None


class OpenDataFiles(BaseModel):
    """Model subclass for Zenodo file id."""

    id: str | None = None


class OpenDataMetadata(BaseModel):
    """Model subclass for Zenodo metadata dict."""

    title: str | None = None
    description: str | None = None
    creators: list[OpenDataCreatorsOrContributors] = []
    contributors: list[OpenDataCreatorsOrContributors] = []
    keywords: list[str] = []
    related_identifiers: list[OpenDataRelateditdentifiers] = []
    language: str | None = None
    license: OpenDataLicense | None = None


class OpenDataParentResource(BaseModel):
    """Model class for a Zenodo record as resource parent."""

    conceptrecid: str
    id: int
    modified: str | None = None
    conceptdoi: str | None = None
    metadata: OpenDataMetadata | None = None


class OpenDataResourceVersion(BaseModel):
    """Model class for Versions of a record."""

    id: int
    conceptrecid: str
    created: str | None = None
    doi_url: str | None = None
    metadata: OpenDataMetadata | None = None
    files: list[OpenDataFiles] = []
    modified: str | None = None
