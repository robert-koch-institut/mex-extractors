from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.exceptions import EmptySearchResultError
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.sorters import topological_sort
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganizationalUnit
    from mex.common.types import MergedOrganizationalUnitIdentifier


def get_extracted_organizational_units() -> list[ExtractedOrganizationalUnit]:
    """Extract, transform and load the organigram, then group unit IDs by synonym.

    Returns:
        Lookup of organizational unit identifiers by synonym
    """
    rki_organization_id = get_wikidata_extracted_organization_id_by_name("RKI")
    if not rki_organization_id:
        msg = "RKI wikidata organization not found"
        raise EmptySearchResultError(msg)
    organigram_units = extract_organigram_units()
    extracted_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        get_extracted_primary_source_id_by_name("organigram"),
        rki_organization_id,
    )
    topological_sort(
        extracted_organizational_units,
        "stableTargetId",
        parent_key="parentUnit",
    )
    load(extracted_organizational_units)
    return extracted_organizational_units


@lru_cache(maxsize=1)
def _get_cached_unit_merged_ids_by_synonyms() -> dict[
    str, list[MergedOrganizationalUnitIdentifier]
]:
    """Extract, transform and load the organigram, then group unit IDs by synonym.

    Returns:
        Lookup of organizational unit identifiers by synonym
    """
    extracted_organizational_units = get_extracted_organizational_units()
    return get_unit_merged_ids_by_synonyms(extracted_organizational_units)


def get_unit_merged_id_by_synonym(
    synonym: str | None,
) -> list[MergedOrganizationalUnitIdentifier] | None:
    """Search a merged organizational unit id by synonym.

    Args:
        synonym: synonym of organizational unit

    Returns:
        list of merged organizational unit ids if found else None
    """
    if not synonym:
        return None

    unit_merged_ids_by_synonyms = _get_cached_unit_merged_ids_by_synonyms()
    return unit_merged_ids_by_synonyms.get(synonym, None)
