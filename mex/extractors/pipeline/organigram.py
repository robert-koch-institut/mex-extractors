from dagster import asset

from mex.common.models import ExtractedOrganizationalUnit
from mex.common.models.organization import ExtractedOrganization
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.sorters import topological_sort


@asset(group_name="default")
def extracted_organizational_units(
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedOrganizationalUnit]:
    """Extract organizational units."""
    organigram_units = extract_organigram_units()
    mex_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units,
        get_extracted_primary_source_id_by_name("organigram"),
        extracted_organization_rki,
    )
    topological_sort(
        mex_organizational_units,
        "stableTargetId",
        parent_key="parentUnit",
    )
    load(mex_organizational_units)
    return mex_organizational_units


@asset(group_name="default")
def unit_stable_target_ids_by_synonym(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Group organizational units by their synonym."""
    return {
        synonym: MergedOrganizationalUnitIdentifier(merged_id)
        for synonym, merged_id in get_unit_merged_ids_by_synonyms(
            extracted_organizational_units
        ).items()
    }
