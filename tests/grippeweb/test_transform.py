from typing import TYPE_CHECKING, Any

import pytest

from mex.common.models import ExtractedLocation, ExtractedOrganization
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.grippeweb.extract import extract_grippeweb_organizations
from mex.extractors.grippeweb.transform import (
    get_or_create_external_partner,
    transform_grippeweb_access_platform_to_extracted_access_platform,
    transform_grippeweb_resource_mapping_to_extracted_resource,
    transform_grippeweb_resource_mappings_to_extracted_resources,
    transform_grippeweb_variable_group_to_extracted_variable_groups,
    transform_grippeweb_variable_to_extracted_variables,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ExtractedAccessPlatform,
        ExtractedOrganizationalUnit,
        ExtractedPerson,
        ExtractedResource,
        ExtractedVariableGroup,
        ResourceMapping,
        VariableGroupMapping,
        VariableMapping,
    )


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_grippeweb_access_platform_to_extracted_access_platform(
    grippeweb_access_platform: AccessPlatformMapping,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    extracted_access_platform = (
        transform_grippeweb_access_platform_to_extracted_access_platform(
            grippeweb_access_platform
        )
    )
    fg99_id = mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
    expected = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("grippeweb"),
        "identifierInPrimarySource": "item-487",
        "contact": [fg99_id],
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "challenge someone beyond"}],
        "unitInCharge": [fg99_id],
        "identifier": "flYKd1yG2tpZrOXw3eCR4n",
        "stableTargetId": "dliAfHO1Hb5FauU149LKvk",
    }
    dumped = extracted_access_platform.model_dump(
        exclude_none=True, exclude_defaults=True
    )
    assert dumped == expected


@pytest.mark.usefixtures("mocked_wikidata", "mocked_wikidata_locations")
def test_transform_grippeweb_resource_mapping_to_extracted_resource(  # noqa: PLR0913, PLR0917
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    juturna_felicitas: ExtractedPerson,
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    germany = ExtractedLocation(
        hadPrimarySource=get_extracted_primary_source_id_by_name("wikidata"),
        identifierInPrimarySource="Q183",
        name=[Text(value="Ort Q183", language=TextLanguage.DE)],
    )
    parent_mapping = grippeweb_resource_mappings[0]
    parent_resource = transform_grippeweb_resource_mapping_to_extracted_resource(
        parent_mapping,
        grippeweb_extracted_access_platform,
        {"Juturna Felicitás": juturna_felicitas.stableTargetId},
        grippeweb_merged_organization_ids_by_query_str,
        grippeweb_merged_contact_point_id_by_email,
    )
    external_partner = ExtractedOrganization(
        officialName="RKI",
        identifierInPrimarySource="RKI",
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
    )
    fg99_id = mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
    dumped = parent_resource.model_dump(exclude_none=True, exclude_defaults=True)
    # values copied verbatim from the mapping are checked against the mapping
    for field in (
        "description",
        "documentation",
        "hasCodeValues",
        "hasLegalBasis",
        "hasPurposeDescription",
        "keyword",
        "method",
        "methodDescription",
        "populationCoverage",
        "resourceTypeSpecific",
        "rights",
    ):
        assert dumped.pop(field) == [
            value.model_dump(exclude_none=True, exclude_defaults=True)
            for value in getattr(parent_mapping, field).get_static_values()
        ], field
    assert dumped == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("grippeweb"),
        "identifierInPrimarySource": "934421",
        "accessPlatform": [grippeweb_extracted_access_platform.stableTargetId],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-12",
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contact": [grippeweb_merged_contact_point_id_by_email["ContactC@rki.de"]],
        "contributingUnit": [fg99_id],
        "contributor": [juturna_felicitas.stableTargetId],
        "externalPartner": [external_partner.stableTargetId],
        "geopoliticalResolution": [
            "https://mex.rki.de/item/geopolitical-resolution-6",
            "https://mex.rki.de/item/geopolitical-resolution-7",
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "hasPurpose": [
            "https://mex.rki.de/item/purpose-31",
            "https://mex.rki.de/item/purpose-13",
            "https://mex.rki.de/item/purpose-33",
            "https://mex.rki.de/item/purpose-21",
            "https://mex.rki.de/item/purpose-12",
            "https://mex.rki.de/item/purpose-13",
            "https://mex.rki.de/item/purpose-13",
        ],
        "language": ["https://mex.rki.de/item/language-6"],
        "minTypicalAge": 2,
        "publisher": [
            grippeweb_merged_organization_ids_by_query_str["Robert Koch-Institut"]
        ],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-6"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-17"],
        "spatial": [germany.stableTargetId],
        "start": ["2024"],
        "stateOfDataProcessing": ["https://mex.rki.de/item/data-processing-state-4"],
        "theme": ["https://mex.rki.de/item/theme-25"],
        "title": [{"value": "second evening", "language": "de"}],
        "unitInCharge": [fg99_id],
        "identifier": "dVOL0CJEif2rpEX4FBD5UH",
        "stableTargetId": "ckeyte9YRrbeB3hQDbqFex",
    }


@pytest.mark.usefixtures("mocked_wikidata", "mocked_wikidata_locations")
def test_transform_grippeweb_resource_mappings_to_extracted_resources(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_extracted_access_platform: ExtractedAccessPlatform,
    grippeweb_merged_organization_ids_by_query_str: dict[
        str, MergedOrganizationIdentifier
    ],
    grippeweb_merged_contact_point_id_by_email: dict[str, MergedContactPointIdentifier],
) -> None:
    parent_resource, child_resource = (
        transform_grippeweb_resource_mappings_to_extracted_resources(
            grippeweb_resource_mappings[0],
            grippeweb_resource_mappings[1],
            grippeweb_extracted_access_platform,
            {},
            grippeweb_merged_organization_ids_by_query_str,
            grippeweb_merged_contact_point_id_by_email,
        )
    )
    assert parent_resource.identifierInPrimarySource == "934421"
    assert child_resource.identifierInPrimarySource == "547063"
    assert parent_resource.relatedResource == []
    assert child_resource.relatedResource == [parent_resource.stableTargetId]
    assert child_resource.externalPartner == []


@pytest.mark.usefixtures("mocked_wikidata")
def test_get_or_create_external_partner_uses_wikidata_match(
    grippeweb_resource_mappings: list[ResourceMapping],
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    parent_mapping, child_mapping = grippeweb_resource_mappings
    organizations = extract_grippeweb_organizations(grippeweb_resource_mappings)
    assert "RKI" in organizations

    external_partner = get_or_create_external_partner(parent_mapping, organizations)

    created_fallback = ExtractedOrganization(
        officialName="RKI",
        identifierInPrimarySource="RKI",
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
    )
    assert external_partner == [wikidata_extracted_organization.stableTargetId]
    assert external_partner != [created_fallback.stableTargetId]
    # grippeweb-plus maps no external partner
    assert get_or_create_external_partner(child_mapping, organizations) is None


def test_transform_grippeweb_variable_group_to_extracted_variable_groups(
    grippeweb_variable_group: VariableGroupMapping,
    mocked_grippeweb_sql_tables: dict[str, dict[str, list[Any]]],
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> None:
    extracted_variable_groups = (
        transform_grippeweb_variable_group_to_extracted_variable_groups(
            grippeweb_variable_group,
            mocked_grippeweb_sql_tables,
            grippeweb_extracted_parent_resource,
        )
    )
    expected = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("grippeweb"),
        "identifierInPrimarySource": "vActualQuestion",
        "containedBy": [grippeweb_extracted_parent_resource.stableTargetId],
        "label": [{"value": "weit versuchen schlimm", "language": "en"}],
        "identifier": "bzpTa62vHvug9Dyn9mpxy3",
        "stableTargetId": "bTfHECQk70fIkx5pBJ8X9P",
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
) -> None:
    extracted_variables = transform_grippeweb_variable_to_extracted_variables(
        grippeweb_variable,
        extracted_variable_groups,
        mocked_grippeweb_sql_tables,
        grippeweb_extracted_parent_resource,
    )
    question_group, weekly_group, master_group = (
        group.stableTargetId for group in extracted_variable_groups
    )
    variables_by_label = {
        variable.identifierInPrimarySource: variable for variable in extracted_variables
    }
    assert list(variables_by_label) == [
        "Id",
        "StartedOn",
        "FinishedOn",
        "RepeatAfterDays",
        "Title",
        "GuidTeilnehmer",
        "Geschlecht",
        "Bundesland_TN",
    ]
    expected = {
        "belongsTo": [question_group],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("grippeweb"),
        "identifier": "c9V6dLlCNErsesRalzCRd6",
        "identifierInPrimarySource": "Id",
        "label": [{"value": "Id"}],
        "stableTargetId": "hc5GA0AvxEDVTAhHu2dnKN",
        "usedIn": [grippeweb_extracted_parent_resource.stableTargetId],
    }
    assert (
        variables_by_label["Id"].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
    # column listed in valueSet for a single table
    assert variables_by_label["Title"].belongsTo == [question_group]
    assert variables_by_label["Title"].valueSet == [
        "COVID-19-Impfung",
        "Influenza-Impfung",
    ]
    # column listed in valueSet for both MEx views, deduplicated across tables
    assert variables_by_label["Geschlecht"].belongsTo == [master_group, weekly_group]
    assert variables_by_label["Geschlecht"].valueSet == ["Diverse", "Female"]
    # null cells are not part of the value set
    assert variables_by_label["Bundesland_TN"].belongsTo == [master_group]
    assert variables_by_label["Bundesland_TN"].valueSet == ["Berlin"]
    # columns not listed in valueSet get none
    assert variables_by_label["GuidTeilnehmer"].valueSet == []
