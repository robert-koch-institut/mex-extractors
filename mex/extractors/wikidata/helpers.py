from functools import cache

from mex.common.exceptions import MExError
from mex.common.models import ExtractedOrganization, OrganizationMapping
from mex.common.types import MergedOrganizationIdentifier
from mex.common.wikidata.extract import (
    _get_organization_details,
)
from mex.common.wikidata.transform import (
    transform_wikidata_organization_to_extracted_organization,
)
from mex.extractors.primary_source.helpers import load_extracted_primary_source_by_name
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


@cache
def get_wikidata_organization_by_id(wikidata_id: str) -> ExtractedOrganization | None:
    """Get and load a wikidata item details by its ID.

    Args:
        wikidata_id: wikidata ID of organization

    Returns:
        extracted organization if found in wikidata
    """
    wikidata_organization = _get_organization_details(wikidata_id.split("/")[-1])
    wikidata_primary_source = load_extracted_primary_source_by_name("wikidata")
    if not wikidata_primary_source:
        msg = "Primary source for wikidata not found"
        raise MExError(msg)
    if (
        extracted_organization
        := transform_wikidata_organization_to_extracted_organization(
            wikidata_organization, wikidata_primary_source
        )
    ):
        load([extracted_organization])
        return extracted_organization
    return None


@cache
def get_wikidata_organization_ids_by_label() -> dict[str, str]:
    """Extract and transform synopse resource default values."""
    settings = Settings.get()
    organization_mapping = OrganizationMapping.model_validate(
        load_yaml(settings.wikidata.mapping_path / "organization.yaml")
    )
    return {
        value: rule.setValues
        for rule in organization_mapping.identifierInPrimarySource[0].mappingRules
        if rule.setValues and rule.forValues
        for value in rule.forValues
    }


@cache
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
    wikidata_organization_ids_by_label = get_wikidata_organization_ids_by_label()
    if (name in wikidata_organization_ids_by_label) and (
        extracted_organization := get_wikidata_organization_by_id(
            wikidata_organization_ids_by_label[name]
        )
    ):
        return extracted_organization.stableTargetId
    return None
