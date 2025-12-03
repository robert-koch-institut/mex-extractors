from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.voxco.model import VoxcoVariable
from mex.extractors.voxco.transform import (
    transform_voxco_resource_mappings_to_extracted_resources,
    transform_voxco_variable_mappings_to_extracted_variables,
)


def test_transform_voxco_resource_mappings_to_extracted_resources(
    voxco_resource_mappings: list[ResourceMapping],
    voxco_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    voxco_extracted_persons: list[ExtractedPerson],
    extracted_organization_rki: ExtractedOrganization,
    international_projects_extracted_activities: list[ExtractedActivity],
) -> None:
    resource_dict = transform_voxco_resource_mappings_to_extracted_resources(
        voxco_resource_mappings,
        voxco_merged_organization_ids_by_query_string,
        voxco_extracted_persons,
        extracted_organization_rki,
        international_projects_extracted_activities,
    )
    expected = {
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("voxco")),
        "identifierInPrimarySource": "voxco-plus",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "externalPartner": [
            str(voxco_merged_organization_ids_by_query_string["Robert Koch-Institut"])
        ],
        "contact": [str(voxco_extracted_persons[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-37"],
        "title": [{"value": "voxco-Plus", "language": TextLanguage.DE}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
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
    voxco_extracted_resources_by_str: dict[str, ExtractedResource],
    voxco_variables: dict[str, list[VoxcoVariable]],
) -> None:
    extracted_variables = transform_voxco_variable_mappings_to_extracted_variables(
        voxco_extracted_resources_by_str,
        voxco_variables,
    )
    expected = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("voxco"),
        "identifierInPrimarySource": "50614",
        "label": [{"value": "Monat"}],
        "usedIn": [voxco_extracted_resources_by_str["voxco-plus"].stableTargetId],
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
