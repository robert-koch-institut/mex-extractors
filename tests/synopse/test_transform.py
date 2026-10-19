from typing import TYPE_CHECKING

import pytest

from mex.common.models import ExtractedOrganization
from mex.common.types import (
    MergedAccessPlatformIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TemporalEntity,
    TextLanguage,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.synopse.transform import (
    transform_overviews_to_resource_lookup,
    transform_synopse_data_to_mex_resources,
    transform_synopse_projects_to_mex_activities,
    transform_synopse_studies_into_access_platforms,
    transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables,
    transform_synopse_variables_to_mex_variable_groups,
    transform_synopse_variables_to_mex_variables,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ActivityMapping,
        ExtractedActivity,
        ExtractedOrganizationalUnit,
        ExtractedPerson,
        ExtractedResource,
        ExtractedVariableGroup,
        ResourceMapping,
    )
    from mex.extractors.synopse.models.project import ProjektUndStudienverwaltung
    from mex.extractors.synopse.models.study import MetadatenZuDatensaetzen
    from mex.extractors.synopse.models.study_overview import Datensatzuebersicht
    from mex.extractors.synopse.models.variable import Variablenuebersicht


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_studies_into_access_platforms(
    synopse_access_platform: AccessPlatformMapping,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    expected_access_platform = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("report-server"),
        "identifierInPrimarySource": "t",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-2",
        "alternativeTitle": [
            {"value": "alternative test title", "language": TextLanguage.DE}
        ],
        "contact": ["bFQoRhcVH5DHYc"],
        "description": [{"value": "test description", "language": TextLanguage.DE}],
        "landingPage": [
            {
                "language": TextLanguage.DE,
                "title": "test landing page",
                "url": "https://www.rki.de/test_landing_page",
            }
        ],
        "title": [{"value": "test title", "language": TextLanguage.DE}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "identifier": "caja5lr50xZDp3vqBFy5oN",
        "stableTargetId": "hok9BZyh5ZyU9EWzXUYLqd",
    }

    access_platforms = transform_synopse_studies_into_access_platforms(
        {"contactc@rki.de": MergedContactPointIdentifier.generate(seed=234)},
        synopse_access_platform,
    )
    assert (
        access_platforms.model_dump(exclude_defaults=True) == expected_access_platform
    )


def test_transform_overviews_to_resource_lookup(
    synopse_study_overviews: list[Datensatzuebersicht],
    synopse_resources: list[ExtractedResource],
) -> None:
    lookup = transform_overviews_to_resource_lookup(
        synopse_study_overviews, synopse_resources
    )
    assert lookup["studie1-set2-18"] == synopse_resources[1]


def test_transform_synopse_variables_to_mex_variable_groups(
    synopse_variables_by_thema: dict[str, list[Variablenuebersicht]],
    resources_by_synopse_id: dict[str, ExtractedResource],
    synopse_study_overviews: list[Datensatzuebersicht],
) -> None:
    expected_variable_group = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("report-server"),
        "identifierInPrimarySource": "Krankheiten (1101)-12345-set1-17",
        "containedBy": [resources_by_synopse_id["12345-set1-17"].stableTargetId],
        "label": [{"value": "Krankheiten", "language": TextLanguage.DE}],
        "identifier": "dhDuJd4JuwlAisTlwj4FNa",
        "stableTargetId": "gGYwMcnH67o2Ob2EutHETU",
    }

    variable_groups = transform_synopse_variables_to_mex_variable_groups(
        synopse_variables_by_thema,
        resources_by_synopse_id,
        synopse_study_overviews,
    )
    sorted_variable_groups = sorted(
        variable_groups, key=lambda v: v.identifierInPrimarySource
    )
    assert (
        sorted_variable_groups[0].model_dump(exclude_defaults=True)
        == expected_variable_group
    )


def test_transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
    synopse_variables: list[Variablenuebersicht],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resources_by_synopse_id: dict[str, ExtractedResource],
    synopse_study_overviews: list[Datensatzuebersicht],
) -> None:
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group for group in extracted_variable_groups
    }
    synopse_variables = [
        var for var in synopse_variables if var.textbox5 == "Krankheiten (1101)"
    ]
    variable_group = variable_group_by_identifier_in_primary_source[
        "Krankheiten (1101)-12345-set1-17"
    ]
    expected_variable_one = {
        "belongsTo": [str(variable_group.stableTargetId)],
        "codingSystem": "Health Questionnaire , Frage 18",
        "dataType": "Zahl",
        "hadPrimarySource": str(
            get_extracted_primary_source_id_by_name("report-server")
        ),
        "identifier": "33C9QmrGmQPXKmwaOsXTt",
        "identifierInPrimarySource": "1",
        "label": [{"language": TextLanguage("de"), "value": "Angeborene Fehlbildung"}],
        "stableTargetId": "evkDk6Y3auo0xASIuyWQV9",
        "usedIn": [str(resources_by_synopse_id["12345-set1-17"].stableTargetId)],
        "valueSet": ["Nicht erhoben", "Weiß nicht"],
    }
    expected_variable_two = {  # var 2, missing var label
        "belongsTo": [str(variable_group.stableTargetId)],
        "dataType": "Zahl",
        "hadPrimarySource": str(
            get_extracted_primary_source_id_by_name("report-server")
        ),
        "identifier": "bPinodvpRBzvspGJxbAmS8",
        "label": [{"value": "KHEfiebB", "language": TextLanguage.DE}],
        "stableTargetId": "gqXeDXpVGbbVbXe3BkIeZr",
        "usedIn": [str(resources_by_synopse_id["12345-set1-17"].stableTargetId)],
        "identifierInPrimarySource": "2",
        "valueSet": ["Ja"],
    }
    variables = (
        transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
            synopse_variables,
            variable_group_by_identifier_in_primary_source,
            resources_by_synopse_id,
            synopse_study_overviews,
        )
    )
    assert len(variables) == 2
    assert variables[0].model_dump(exclude_defaults=True) == expected_variable_one
    assert variables[1].model_dump(exclude_defaults=True) == expected_variable_two


def test_transform_synopse_variables_to_mex_variables(
    synopse_variables_by_thema: dict[str, list[Variablenuebersicht]],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resources_by_synopse_id: dict[str, ExtractedResource],
    synopse_study_overviews: list[Datensatzuebersicht],
) -> None:
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group for group in extracted_variable_groups
    }
    variables = transform_synopse_variables_to_mex_variables(
        synopse_variables_by_thema,
        variable_group_by_identifier_in_primary_source,
        resources_by_synopse_id,
        synopse_study_overviews,
    )

    sorted_variables = sorted(variables, key=lambda v: v.identifierInPrimarySource)
    assert len(variables) == 2
    assert sorted_variables[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("report-server"),
        "identifierInPrimarySource": "1",
        "codingSystem": "Health Questionnaire , Frage 18",
        "dataType": "Zahl",
        "label": [{"value": "Angeborene Fehlbildung", "language": TextLanguage.DE}],
        "usedIn": [resources_by_synopse_id["12345-set1-17"].stableTargetId],
        "belongsTo": [
            variable_group_by_identifier_in_primary_source[
                "Krankheiten (1101)-12345-set1-17"
            ].stableTargetId
        ],
        "valueSet": ["Nicht erhoben", "Weiß nicht"],
        "identifier": "33C9QmrGmQPXKmwaOsXTt",
        "stableTargetId": "evkDk6Y3auo0xASIuyWQV9",
    }


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_data_to_mex_resources(  # noqa: PLR0913, PLR0917
    synopse_project: ProjektUndStudienverwaltung,
    synopse_studies: list[MetadatenZuDatensaetzen],
    extracted_activity: ExtractedActivity,
    extracted_organization_rki: ExtractedOrganization,
    synopse_resource: ResourceMapping,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:

    expected_resource = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("report-server"),
        "identifierInPrimarySource": "12345-Titel-17",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "modified": "2022",
        "wasGeneratedBy": extracted_activity.stableTargetId,
        "contact": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"value": "Titel", "language": TextLanguage.DE}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "accessPlatform": ["bFQoRhcVH5DHYe"],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId,
        ],
        "description": [
            {"value": "ein heikles Unterfangen.", "language": TextLanguage.DE}
        ],
        "hasLegalBasis": [
            {"value": "Niemand darf irgendwas.", "language": TextLanguage.DE}
        ],
        "healthCategory": ["https://mex.rki.de/item/health-category-1"],
        "keyword": [
            {"value": "Alkohol", "language": TextLanguage.DE},
            {"value": "Alter und Geschlecht", "language": TextLanguage.DE},
            {"value": "Drogen", "language": TextLanguage.DE},
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [extracted_organization_rki.stableTargetId],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-2"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "resourceTypeSpecific": [
            {"value": "Monitoring-studie", "language": TextLanguage.DE}
        ],
        "rights": [{"value": "Lorem", "language": TextLanguage.DE}],
        "identifier": "c8uxmFTO2QmNb7xmfUReke",
        "stableTargetId": "ecfQkgjG0kk6bqlO0pdCbx",
    }

    resources = transform_synopse_data_to_mex_resources(
        [synopse_studies[0]],
        [synopse_project],
        [extracted_activity],
        extracted_organization_rki,
        synopse_resource,
        MergedAccessPlatformIdentifier.generate(seed=236),
        {"Jane Doe": [MergedPersonIdentifier.generate(seed=237)]},
    )
    assert len(resources) == 1
    assert resources[0].model_dump(exclude_defaults=True) == expected_resource


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_projects_to_mex_activities(
    synopse_projects: list[ProjektUndStudienverwaltung],
    synopse_activity: ActivityMapping,
    synopse_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    roland_resolved: ExtractedPerson,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    synopse_project = synopse_projects[0]
    contributor_merged_ids_by_name = {
        "Roland Resolved": [roland_resolved.stableTargetId]
    }

    assert synopse_project.projektende
    external_partner = ExtractedOrganization(
        officialName="Testpartner",
        identifierInPrimarySource="Testpartner",
        hadPrimarySource=get_extracted_primary_source_id_by_name("report-server"),
    )
    assert synopse_project.projektbeginn

    expected_activity = {
        "abstract": [{"value": synopse_project.beschreibung_studie}],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "documentation": [
            {
                "url": "file:///Z:/Projekte/Dokumentation",
                "title": "- Fragebogen\n- Labor",
            }
        ],
        "end": [str(TemporalEntity(synopse_project.projektende))],
        "externalAssociate": [external_partner.stableTargetId],
        "hadPrimarySource": str(
            get_extracted_primary_source_id_by_name("report-server")
        ),
        "identifier": "bqlLDVupJJHTGbdzXCoCIQ",
        "identifierInPrimarySource": synopse_project.studien_id,
        "involvedPerson": [roland_resolved.stableTargetId],
        "responsibleUnit": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],  # C1
        "involvedUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,  # FG99
            mocked_units_by_identifier_in_primary_source[
                "child-unit"
            ].stableTargetId,  # C1
        ],
        "shortName": [{"value": "BBCCDD_00", "language": TextLanguage.DE}],
        "stableTargetId": "fIwPZ5WUfvpP3V2xdFFnTr",
        "start": [str(TemporalEntity(synopse_project.projektbeginn))],
        "theme": ["https://mex.rki.de/item/theme-36"],
        "title": [{"language": TextLanguage.DE, "value": "studie zu Lorem und Ipsum"}],
    }

    activities = transform_synopse_projects_to_mex_activities(
        synopse_projects,
        contributor_merged_ids_by_name,
        synopse_activity,
        synopse_merged_organization_ids_by_query_string,
    )

    assert len(activities) == 2
    expected_activity["succeeds"] = [activities[0][0].stableTargetId]
    assert activities[1][0].model_dump(exclude_defaults=True) == expected_activity
