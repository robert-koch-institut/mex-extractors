from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.voxco.model import VoxcoVariable
from mex.extractors.voxco.transform import (
    transform_voxco_resource_mappings_to_extracted_resources,
    transform_voxco_variable_mappings_to_extracted_variables,
)


def test_transform_voxco_resource_mappings_to_extracted_resources(  # noqa: PLR0913
    voxco_resource_mappings: list[ResourceMapping],
    voxco_organization_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
    voxco_persons: list[ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    international_projects_extracted_activities: list[ExtractedActivity],
) -> None:
    resource_dict = transform_voxco_resource_mappings_to_extracted_resources(
        voxco_resource_mappings,
        voxco_organization_stable_target_id_by_query,
        voxco_persons,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        extracted_primary_sources["voxco"],
        international_projects_extracted_activities,
    )
    expected = {
        "hadPrimarySource": str(extracted_primary_sources["voxco"].stableTargetId),
        "identifierInPrimarySource": "voxco-plus",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "externalPartner": [
            str(voxco_organization_stable_target_id_by_query["Robert Koch-Institut"])
        ],
        "contact": [str(voxco_persons[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-37"],
        "title": [{"value": "voxco-Plus", "language": TextLanguage.DE}],
        "unitInCharge": [str(unit_stable_target_ids_by_synonym["C1"])],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "description": [
            {
                "value": "Erreger-spezifische Zusatzinformationen",
                "language": TextLanguage.DE,
            }
        ],
        "keyword": [{"value": "Surveillance", "language": TextLanguage.DE}],
        "language": ["https://mex.rki.de/item/language-1"],
        "meshId": [
            "http://id.nlm.nih.gov/mesh/D012140",
            "http://id.nlm.nih.gov/mesh/D012141",
            "http://id.nlm.nih.gov/mesh/D007251",
        ],
        "method": [{"value": "Selbstabstriche", "language": TextLanguage.DE}],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "qualityInformation": [{"value": "description", "language": TextLanguage.DE}],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-2"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-15"],
        "resourceTypeSpecific": [
            {"value": "Nasenabstrich", "language": TextLanguage.DE}
        ],
        "rights": [{"value": "Die Daten", "language": TextLanguage.DE}],
        "spatial": [{"value": "Deutschland", "language": TextLanguage.DE}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
        "wasGeneratedBy": str(
            international_projects_extracted_activities[0].stableTargetId
        ),
    }

    assert (
        resource_dict["voxco-plus"].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


def test_transform_voxco_variable_mappings_to_extracted_variables(
    voxco_resources: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        voxco_resources, voxco_variables, extracted_primary_sources["voxco"]
    )
    expected = {
        "hadPrimarySource": extracted_primary_sources["voxco"].stableTargetId,
        "identifierInPrimarySource": "50614",
        "label": [{"value": "Monat"}],
        "usedIn": [voxco_resources["voxco-plus"].stableTargetId],
        "dataType": "Text",
        "description": [{"value": "Discrete", "language": TextLanguage.DE}],
        "valueSet": ["Januar", "Februar"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert (
        extracted_variables[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
