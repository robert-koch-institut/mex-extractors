import json
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from mex.common.assets.registry import get_assets_connector
from mex.common.logging import logger
from mex.common.wikidata.extract import get_wikidata_locations
from mex.extractors.locations.connector import WikidataSPARQLConnector
from mex.extractors.locations.models import Country
from mex.extractors.settings import ExtractorsSettings

if TYPE_CHECKING:
    from collections.abc import Iterator

    from mex.common.wikidata.models import WikidataLocation


def extract_country_wikidata_ids_from_eu_vocabularies_json() -> list[str]:
    """Read and validate countries from the EU vocabularies countries JSON file."""
    settings = ExtractorsSettings.get()
    connector = get_assets_connector()

    raw_countries = json.loads(connector.read(settings.locations.country_list_path))
    countries = TypeAdapter(list[Country]).validate_python(raw_countries)

    logger.info("extracted %d country EU vocabulary units", len(countries))
    return [
        country.wikidata_id[0]
        .removeprefix("http://www.wikidata.org/entity/")
        .removeprefix("https://www.wikidata.org/wiki/")
        for country in countries
    ]


def extract_region_wikidata_ids() -> list[str]:
    """Query region wikidata ids from Wikidata."""
    connector = WikidataSPARQLConnector.get()

    return connector.extract_wikidata_location_ids()


def extract_wikidata_locations(
    wikidata_ids: list[str],
) -> Iterator[WikidataLocation]:
    """Extract locations from Wikidata by their identifiers."""
    yield from get_wikidata_locations(sorted(wikidata_ids))
