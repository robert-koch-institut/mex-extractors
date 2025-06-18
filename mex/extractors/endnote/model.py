from pydantic import BaseModel


class EndnoteRecord(BaseModel):
    """Model class for endnote record type entities."""

    abstract: str | None
    authors: list[str]
    call_num: str | None
    custom3: str | None
    custom4: str | None
    custom6: str | None
    database: str | None
    electronic_resource_num: str | None
    isbn: str | None
    keyword: list[str]
    language: str | None
    number: str | None
    pages: str | None
    periodical: list[str]
    pub_dates: list[str]
    publisher: str | None
    rec_number: str | None
    ref_type: str
    related_urls: list[str]
    secondary_authors: list[str]
    secondary_title: str | None
    tertiary_authors: list[str]
    title: str
    volume: str | None
    year: str | None
