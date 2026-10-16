from pydantic import BaseModel, Field


class CountryName(BaseModel):
    """Class for country names for EU vocabulary countries."""

    language: str
    value: str


class Country(BaseModel):
    """Class for EU vocabulary countries."""

    name: list[CountryName]
    identifier: list[str]
    wikidata_id: list[str] = Field(alias="wikidataId")
