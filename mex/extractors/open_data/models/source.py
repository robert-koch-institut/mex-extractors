from pydantic import BaseModel


class ZenodoParentRecordMetadata(BaseModel):
    """Model subclass for Zenodo resource parent metadata dict."""

    title: str | None = None
    description: str | None = None
    creators: list[dict[str, str]] = []
    contributors: list[dict[str, str]] = []
    keywords: list[str] = []
    related_identifiers: list[dict[str, str]] = []
    language: str | None = None
    license: dict[str, str] = {}


class ZenodoParentRecordSource(BaseModel):
    """Model class for a Zenodo record as resource parent."""

    conceptrecid: str | None = None
    modified: str | None = None
    id: int | None = None
    conceptdoi: str | None = None
    metadata: ZenodoParentRecordMetadata


class ZenodoRecordVersion(BaseModel):
    """Model class for Versions of a record."""

    created: str | None = None
    doi_url: str | None = None
    downloadURL: str | None = None
    issued: str | None = None
    license: str | None = None
    mediaType: str | None = None
    modified: str | None = None
    title: str | None = None
    affiliation: str | None = None
    name: str | None = None
    orcid: str | None = None
