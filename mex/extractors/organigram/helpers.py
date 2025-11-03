from functools import lru_cache

from mex.common.organigram.extract import (
    extract_organigram_units,
    get_extracted_unit_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import get_wikidata_organization_ids_by_label


@lru_cache(maxsize=1024)
def get_unit_merged_id_by_synonym(
    synonym: str,
) -> MergedOrganizationalUnitIdentifier | None:
    """Search a merged organizational unit id by synonym.

    Args:
        extracted_units: list of extracted units
        synonym: synonym of organizational unit

    Returns:
        merged organizational unit id if found else None
    """
    wikidata_organization_ids_by_label = get_wikidata_organization_ids_by_label()
    organigram_units = extract_organigram_units()
    mex_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        get_extracted_primary_source_id_by_name("organigram"),
        wikidata_organization_ids_by_label("RKI"),
    )
    unit_merged_ids_by_synonyms = get_extracted_unit_by_synonyms(organigram_units)
    if synonym not in unit_merged_ids_by_synonyms:
        return None
    mex_organizational_unit = unit_merged_ids_by_synonyms[synonym]
    load([mex_organizational_unit])
    return mex_organizational_unit
