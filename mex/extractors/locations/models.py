from pydantic import BaseModel, Field


class CountryName(BaseModel):
    language: str
    value: str


class Country(BaseModel):
    name: list[CountryName]
    identifier: list[str]
    wikidata_id: list[str] = Field(alias="wikidataId")
