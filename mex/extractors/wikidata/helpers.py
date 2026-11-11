from functools import lru_cache
from typing import TYPE_CHECKING

from mex.common.models import ExtractedOrganization, OrganizationMapping
from mex.common.wikidata.helpers import get_wikidata_organization_with_locations
from mex.extractors.assets import load_yaml
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from mex.common.types import MergedOrganizationIdentifier


def get_wikidata_organization_by_id(wikidata_id: str) -> ExtractedOrganization | None:
    """Get and load a wikidata item details by its ID.

    Args:
        wikidata_id: wikidata ID of organization

    Returns:
        extracted organization if found in wikidata
    """
    result = get_wikidata_organization_with_locations(wikidata_id)

    if result is None:
        return None

    load([result.organization, *result.locations, result.primary_source])

    return result.organization


# TODO(ND): move this helper to mex-common
@lru_cache(maxsize=1)
def get_wikidata_organization_mapping() -> OrganizationMapping:
    """Load the organization mapping with already defined wikidata ids."""
    settings = ExtractorsSettings.get()
    return OrganizationMapping.model_validate(
        load_yaml(f"{settings.wikidata.mapping_path}/organization.yaml")
    )


def get_wikidata_organization_id_by_label(label: str) -> str | None:
    """Return an already defined wikidata id for the given label, if any."""
    organization_mapping = get_wikidata_organization_mapping()
    return next(
        iter(organization_mapping.identifierInPrimarySource.get_mapped_values(label)),
        None,
    )


@lru_cache(maxsize=1024)
def get_wikidata_extracted_organization_id_by_name(
    name: str,
) -> MergedOrganizationIdentifier | None:
    """Use helper function to look up an organization and return its stableTargetId.

    An organization searched by its Wikidata id on Wikidata and loaded into the
    configured sink. Also it's stable target id is returned.

    Returns:
        ExtractedOrganization stableTargetId if one matching organization is found.
        None if multiple matches / no match is found
    """
    if (wikidata_id := get_wikidata_organization_id_by_label(name)) and (
        extracted_organization := get_wikidata_organization_by_id(wikidata_id)
    ):
        return extracted_organization.stableTargetId
    return None
