from mex.common.models import BaseModel


class OpenDataCreatorsOrContributors(BaseModel):
    """Model subclass for Zenodo metadata Creators or Contributors."""

    name: str
    affiliation: str | None = None
    orcid: str | None = None

    def __hash__(self) -> int:
        """Calculates a hash value to make the object cacheable."""
        return hash(f"{self.name}")


class OpenDataRelatedIdentifiers(BaseModel):
    """Model subclass for Zenodo metadata related identifiers."""

    identifier: str | None = None
    relation: str | None = None


class OpenDataLicenseOrFile(BaseModel):
    """Model subclass for Zenodo metadata license."""

    id: str | None = None


class Links(BaseModel):
    """Model subclass for Zenodo links."""

    self: str | None = None


class OpenDataMetadata(BaseModel):
    """Model subclass for Zenodo metadata dict."""

    description: str | None = None
    creators: list[OpenDataCreatorsOrContributors] = []
    contributors: list[OpenDataCreatorsOrContributors] = []
    keywords: list[str] = []
    related_identifiers: list[OpenDataRelatedIdentifiers] = []
    language: str | None = None
    license: OpenDataLicenseOrFile
    publication_date: str | None = None


class OpenDataParentResource(BaseModel):
    """Model class for a Zenodo record as resource parent."""

    modified: str | None = None
    id: int
    conceptrecid: str
    conceptdoi: str | None = None
    metadata: OpenDataMetadata
    title: str | None = None
    files: list[OpenDataLicenseOrFile]


class OpenDataResourceVersion(BaseModel):
    """Model class for Versions of a record."""

    created: str | None = None
    id: int
    metadata: OpenDataMetadata


class OpenDataVersionFiles(BaseModel):
    """Model subclass for Zenodo files."""

    key: str | None = None
    file_id: str
    mimetype: str | None = None
    links: Links
    created: str | None = None
    updated: str | None = None
