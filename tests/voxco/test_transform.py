from typing import Any

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.voxco.model import VoxcoVariable
from mex.voxco.transform import (
    transform_voxco_resource_mappings_to_extracted_resources,
    transform_voxco_variable_mappings_to_extracted_variables,
)


def test_transform_voxco_resource_mappings_to_extracted_resources(
    voxco_resource_mappings: list[dict[str, Any]],
    organization_stable_target_id_by_query_voxco: dict[
        str, MergedOrganizationIdentifier
    ],
    extracted_mex_persons_voxco: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_sources: ExtractedPrimarySource,
    extracted_international_projects_activities: list[ExtractedActivity],
) -> None:
    resource_dict = transform_voxco_resource_mappings_to_extracted_resources(
        voxco_resource_mappings,
        organization_stable_target_id_by_query_voxco,
        extracted_mex_persons_voxco,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        extracted_primary_sources["voxco"],
        extracted_international_projects_activities,
    )
    expected = {
        "hadPrimarySource": extracted_primary_sources["voxco"].stableTargetId,
        "identifierInPrimarySource": "voxco-plus",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "externalPartner": [
            organization_stable_target_id_by_query_voxco["Robert Koch-Institut"]
        ],
        "contact": [extracted_mex_persons_voxco[0].stableTargetId],
        "theme": ["https://mex.rki.de/item/theme-35"],
        "title": [{"value": "voxco-Plus", "language": "de"}],
        "unitInCharge": [unit_stable_target_ids_by_synonym["C1"]],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "description": [
            {"value": "Erreger-spezifische Zusatzinformationen", "language": "de"}
        ],
        "keyword": [{"value": "Surveillance", "language": "de"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "meshId": [
            "http://id.nlm.nih.gov/mesh/D012140",
            "http://id.nlm.nih.gov/mesh/D012141",
            "http://id.nlm.nih.gov/mesh/D007251",
        ],
        "method": [{"value": "Selbstabstriche", "language": "de"}],
        "publisher": [extracted_organization_rki.stableTargetId],
        "qualityInformation": [{"value": "description", "language": "de"}],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-10"],
        "rights": [{"value": "Die Daten", "language": "de"}],
        "spatial": [{"value": "Deutschland", "language": "de"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
        "wasGeneratedBy": extracted_international_projects_activities[0].stableTargetId,
    }

    assert (
        resource_dict["voxco-plus"].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


def test_transform_voxco_variable_mappings_to_extracted_variables(
    extracted_voxco_resources: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
    extracted_primary_sources: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        extracted_voxco_resources, voxco_variables, extracted_primary_sources["voxco"]
    )
    expected = {
        "hadPrimarySource": extracted_primary_sources["voxco"].stableTargetId,
        "identifierInPrimarySource": "50614",
        "label": [{"value": "Monat"}],
        "usedIn": [extracted_voxco_resources["voxco-plus"].stableTargetId],
        "description": [{"value": "Discrete", "language": "de"}],
        "valueSet": ["Januar", "Februar"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert (
        extracted_variables[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
