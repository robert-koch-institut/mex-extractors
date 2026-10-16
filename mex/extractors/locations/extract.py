import json

from mex.common.assets.registry import get_assets_connector
from mex.common.logging import logger
from mex.extractors.locations.models import Country
from mex.extractors.settings import ExtractorsSettings


def extract_countries_from_eu_vocabularies_json() -> list[Country]:
    """Read and validate countries from the EU vocabularies countries JSON file."""
    settings = ExtractorsSettings.get()
    connector = get_assets_connector()
    raw_countries = json.loads(connector.read(settings.locations.country_list_path))
    logger.info("extracted %s country EU vocabularies units", len(raw_countries))
    return [Country.model_validate(raw) for raw in raw_countries]
