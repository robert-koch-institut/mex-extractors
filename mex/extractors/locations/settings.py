from pydantic import Field

from mex.common.models import BaseModel


class LocationsSettings(BaseModel):
    """Settings definition for the infection protection act data."""

    country_list_path: str = Field(
        "raw-data/eu-vocabularies/countries-and-territories.json",
        description=(
            "Path of the country list in json format, relative to `asset_dir`"
        ),
    )
    sparql_url: str = Field(
        "https://query.wikidata.org/sparql", description="Wikidata SPARQL endpoint"
    )
