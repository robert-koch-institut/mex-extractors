from pydantic import BaseModel, Field


class Country(BaseModel):
    """Class for EU vocabulary countries."""

    wikidata_id: list[str] = Field(
        alias="wikidataId",
        min_length=1,
        max_length=1,
    )
