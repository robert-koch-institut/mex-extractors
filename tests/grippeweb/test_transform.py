from typing import Any

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Email,
    LinkLanguage,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.grippeweb.transform import (
    transform_grippeweb_access_platform_to_extracted_access_platform,
    transform_grippeweb_resource_mappings_to_dict,
    transform_grippeweb_resource_mappings_to_extracted_resources,
    transform_grippeweb_variable_group_to_extracted_variable_groups,
    transform_grippeweb_variable_to_extracted_variables,
)


def test_transform_grippeweb_access_platform_to_extracted_access_platform(
    grippeweb_access_platform: AccessPlatformMapping,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
) -> None:
    extracted_access_platform = (
        transform_grippeweb_access_platform_to_extracted_access_platform(
            grippeweb_access_platform,
            unit_stable_target_ids_by_synonym,
            extracted_primary_sources["grippeweb"],
            extracted_mex_persons_grippeweb,
        )
    )
    expected = {
        "hadPrimarySource": extracted_primary_sources["grippeweb"].stableTargetId,
        "identifierInPrimarySource": "primary-source",
        "contact": [extracted_mex_persons_grippeweb[0].stableTargetId],
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "primary-source", "language": "en"}],
        "unitInCharge": [unit_stable_target_ids_by_synonym["C1"]],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert (
        extracted_access_platform.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


def test_transform_grippeweb_resource_mappings_to_dict(  # noqa: PLR0913
    grippeweb_resource_mappings: list[ResourceMapping],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> None:
    parent_resource, _ = transform_grippeweb_resource_mappings_to_dict(
        grippeweb_resource_mappings,
        unit_stable_target_ids_by_synonym,
        grippeweb_extracted_access_platform,
        extracted_primary_sources["grippeweb"],
        extracted_mex_persons_grippeweb,
        grippeweb_organization_ids_by_query_string,
        extracted_mex_functional_units_grippeweb,
    )
    expected = {
        "hadPrimarySource": str(extracted_primary_sources["grippeweb"].stableTargetId),
        "identifierInPrimarySource": "grippeweb",
        "accessPlatform": [str(grippeweb_extracted_access_platform.stableTargetId)],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contact": [str(extracted_mex_functional_units_grippeweb["contactc@rki.de"])],
        "contributingUnit": [str(unit_stable_target_ids_by_synonym["C1"])],
        "contributor": [str(extracted_mex_persons_grippeweb[0].stableTargetId)],
        "created": "2011",
        "description": [{"value": "GrippeWeb", "language": TextLanguage.DE}],
        "documentation": [
            {
                "language": LinkLanguage.DE,
                "title": "RKI Website",
                "url": "https://www.rki.de",
            }
        ],
        "externalPartner": ["hIOhGEYgr1ZO2hmdN23zeg"],
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Bei dem Verfahren.",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "icd10code": ["J00-J99"],
        "keyword": [{"value": "Citizen Science", "language": TextLanguage.EN}],
        "language": ["https://mex.rki.de/item/language-1"],
        "meshId": ["http://id.nlm.nih.gov/mesh/D012140"],
        "method": [{"value": "Online-Befragung", "language": TextLanguage.DE}],
        "methodDescription": [
            {"value": "Online-Surveillanceintrument", "language": TextLanguage.DE}
        ],
        "minTypicalAge": 0,
        "populationCoverage": [
            {
                "language": TextLanguage.DE,
                "value": "Alle Personen.",
            }
        ],
        "publisher": [
            str(grippeweb_organization_ids_by_query_string["Robert Koch-Institut"])
        ],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "resourceTypeSpecific": [
            {
                "value": "bevölkerungsbasierte Surveillancedaten",
                "language": TextLanguage.DE,
            }
        ],
        "rights": [{"value": "Verfahren", "language": TextLanguage.DE}],
        "sizeOfDataBasis": "Meldungen",
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stateOfDataProcessing": ["https://mex.rki.de/item/data-processing-state-1"],
        "temporal": "seit 2011",
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "GrippeWeb", "language": TextLanguage.DE}],
        "unitInCharge": [str(unit_stable_target_ids_by_synonym["C1"])],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert (
        parent_resource.model_dump(exclude_none=True, exclude_defaults=True) == expected
    )


def test_transform_grippeweb_resource_mappings_to_extracted_resources(  # noqa: PLR0913
    grippeweb_resource_mappings: list[ResourceMapping],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_mex_persons_grippeweb: list[ExtractedPerson],
    grippeweb_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    extracted_mex_functional_units_grippeweb: dict[Email, MergedContactPointIdentifier],
) -> None:
    parent_resource, child_resource = (
        transform_grippeweb_resource_mappings_to_extracted_resources(
            grippeweb_resource_mappings,
            unit_stable_target_ids_by_synonym,
            grippeweb_extracted_access_platform,
            extracted_primary_sources["grippeweb"],
            extracted_mex_persons_grippeweb,
            grippeweb_organization_ids_by_query_string,
            extracted_mex_functional_units_grippeweb,
        )
    )
    assert child_resource.isPartOf == [parent_resource.stableTargetId]


def test_transform_grippeweb_variable_group_to_extracted_variable_groups(
    grippeweb_variable_group: VariableGroupMapping,
    mocked_grippeweb_sql_tables: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variable_groups = (
        transform_grippeweb_variable_group_to_extracted_variable_groups(
            grippeweb_variable_group,
            mocked_grippeweb_sql_tables,
            grippeweb_extracted_parent_resource,
            extracted_primary_sources["grippeweb"],
        )
    )
    expected = {
        "hadPrimarySource": extracted_primary_sources["grippeweb"].stableTargetId,
        "identifierInPrimarySource": "vActualQuestion",
        "containedBy": [grippeweb_extracted_parent_resource.stableTargetId],
        "label": [{"value": "Additional Questions", "language": "en"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert (
        extracted_variable_groups[0].model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )


def test_transform_grippeweb_variable_to_extracted_variables(
    grippeweb_variable: VariableMapping,
    extracted_variable_groups: list[ExtractedVariableGroup],
    mocked_grippeweb_sql_tables: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_variables = transform_grippeweb_variable_to_extracted_variables(
        grippeweb_variable,
        extracted_variable_groups,
        mocked_grippeweb_sql_tables,
        grippeweb_extracted_parent_resource,
        extracted_primary_sources["grippeweb"],
    )
    extracted_variables[0].valueSet = sorted(extracted_variables[0].valueSet)
    expected = {
        "belongsTo": [extracted_variable_groups[0].stableTargetId],
        "hadPrimarySource": extracted_primary_sources["grippeweb"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "Id",
        "label": [{"value": "Id"}],
        "stableTargetId": Joker(),
        "usedIn": [grippeweb_extracted_parent_resource.stableTargetId],
        "valueSet": ["AAA", "BBB"],
    }
    assert (
        extracted_variables[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
