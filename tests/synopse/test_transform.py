import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedAccessPlatformIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TemporalEntity,
    TextLanguage,
)
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    transform_overviews_to_resource_lookup,
    transform_synopse_data_to_mex_resources,
    transform_synopse_projects_to_mex_activities,
    transform_synopse_studies_into_access_platforms,
    transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables,
    transform_synopse_variables_to_mex_variable_groups,
    transform_synopse_variables_to_mex_variables,
)


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_studies_into_access_platforms(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_access_platform: AccessPlatformMapping,
) -> None:
    unit_merged_ids_by_synonym = {
        "C1": MergedOrganizationalUnitIdentifier.generate(seed=234)
    }
    expected_access_platform = {
        "hadPrimarySource": "bVro4tpIg0kIjZubkhTmtE",
        "identifierInPrimarySource": "t",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-2",
        "alternativeTitle": [{"value": "alternative test title", "language": "de"}],
        "contact": ["bFQoRhcVH5DHYc"],
        "description": [{"value": "test description", "language": "de"}],
        "landingPage": [
            {
                "language": "de",
                "title": "test landing page",
                "url": "https://www.rki.de/test_landing_page",
            }
        ],
        "title": [{"value": "test title", "language": "de"}],
        "unitInCharge": ["bFQoRhcVH5DHYc"],
        "identifier": "caja5lr50xZDp3vqBFy5oN",
        "stableTargetId": "hok9BZyh5ZyU9EWzXUYLqd",
    }

    access_platforms = transform_synopse_studies_into_access_platforms(
        unit_merged_ids_by_synonym,
        extracted_primary_sources["report-server"],
        {"email@email.de": MergedContactPointIdentifier.generate(seed=234)},
        synopse_access_platform,
    )
    assert (
        access_platforms.model_dump(exclude_defaults=True) == expected_access_platform
    )


def test_transform_overviews_to_resource_lookup(
    synopse_study_overviews: list[SynopseStudyOverview],
    synopse_resources: list[ExtractedResource],
) -> None:
    lookup = transform_overviews_to_resource_lookup(
        synopse_study_overviews, synopse_resources
    )
    assert lookup["synopse1"] == synopse_resources[1]


def test_transform_synopse_variables_to_mex_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    resources_by_synopse_id: dict[str, ExtractedResource],
) -> None:
    expected_variable_group = {
        "hadPrimarySource": "bVro4tpIg0kIjZubkhTmtE",
        "identifierInPrimarySource": "Krankheiten (1101)-studie1-set1-17",
        "containedBy": [Joker(), Joker()],
        "label": [{"value": "Krankheiten", "language": "de"}],
        "identifier": "hSo6lOFWAQYpRHZzr5MhTN",
        "stableTargetId": "d95T1lCg7ZSlWgqOxBQAAr",
    }

    variable_groups = list(
        transform_synopse_variables_to_mex_variable_groups(
            synopse_variables_by_thema,
            extracted_primary_sources["report-server"],
            resources_by_synopse_id,
        )
    )
    sorted_variable_groups = sorted(
        variable_groups, key=lambda v: v.identifierInPrimarySource
    )
    assert (
        sorted_variable_groups[0].model_dump(exclude_defaults=True)
        == expected_variable_group
    )


def test_transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
    synopse_variables: list[SynopseVariable],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resources_by_synopse_id: dict[str, ExtractedResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group for group in extracted_variable_groups
    }
    synopse_variables = [
        var
        for var in synopse_variables
        if var.thema_und_fragebogenausschnitt == "Krankheiten (1101)"
    ]
    variable_group = variable_group_by_identifier_in_primary_source[
        "Krankheiten (1101)-studie1-set2-18"
    ]
    expected_variable_one = {
        "belongsTo": [str(variable_group.stableTargetId)],
        "codingSystem": "Health Questionnaire , Frage 18",
        "dataType": "Zahl",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "1",
        "label": [{"language": TextLanguage("de"), "value": "Angeborene Fehlbildung"}],
        "stableTargetId": Joker(),
        "usedIn": [str(resources_by_synopse_id["1"].stableTargetId)],
        "valueSet": ["Nicht erhoben", "Weiß nicht"],
    }
    expected_variable_two = {  # var 2, missing var label
        "belongsTo": [str(variable_group.stableTargetId)],
        "dataType": "Zahl",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "label": [{"value": "KHEfiebB", "language": TextLanguage.DE}],
        "stableTargetId": Joker(),
        "usedIn": [str(resources_by_synopse_id["2"].stableTargetId)],
        "identifierInPrimarySource": "2",
        "valueSet": ["Ja"],
    }
    variables = list(
        transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
            synopse_variables,
            variable_group,
            resources_by_synopse_id,
            extracted_primary_sources["report-server"],
        )
    )
    assert len(variables) == 2
    assert variables[0].model_dump(exclude_defaults=True) == expected_variable_one
    assert variables[1].model_dump(exclude_defaults=True) == expected_variable_two


def test_transform_synopse_variables_to_mex_variables(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resources_by_synopse_id: dict[str, ExtractedResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    variables = list(
        transform_synopse_variables_to_mex_variables(
            synopse_variables_by_thema,
            extracted_variable_groups,
            resources_by_synopse_id,
            extracted_primary_sources["report-server"],
        )
    )

    sorted_variables = sorted(variables, key=lambda v: v.identifierInPrimarySource)
    assert len(variables) == 2
    assert sorted_variables[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "bVro4tpIg0kIjZubkhTmtE",
        "identifierInPrimarySource": "1",
        "codingSystem": "Health Questionnaire , Frage 18",
        "dataType": "Zahl",
        "label": [{"value": "Angeborene Fehlbildung", "language": "de"}],
        "usedIn": [Joker()],
        "belongsTo": [Joker()],
        "valueSet": ["Nicht erhoben", "Weiß nicht"],
        "identifier": "33C9QmrGmQPXKmwaOsXTt",
        "stableTargetId": "evkDk6Y3auo0xASIuyWQV9",
    }


def test_transform_synopse_data_to_mex_resources(  # noqa: PLR0913
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_project: SynopseProject,
    synopse_studies: list[SynopseStudy],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    extracted_activity: ExtractedActivity,
    extracted_organization: list[ExtractedOrganization],
    synopse_resource: ResourceMapping,
) -> None:
    unit_merged_ids_by_synonym = {
        "C1": MergedOrganizationalUnitIdentifier.generate(seed=234)
    }
    expected_resource = {
        "accessPlatform": [str(Identifier.generate(seed=236))],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": ["bFQoRhcVH5DHYc"],
        "contributor": [str(extracted_activity.involvedPerson[0])],
        "created": "2022",
        "description": [
            {"language": TextLanguage.DE, "value": "ein heikles Unterfangen."}
        ],
        "documentation": [{"url": "file:///Z:/foo/bar"}],
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Niemand darf irgendwas.",
            },
        ],
        "identifier": Joker(),
        "identifierInPrimarySource": ("12345-Titel-17"),
        "keyword": [
            {"language": TextLanguage.DE, "value": "Alkohol"},
            {"language": TextLanguage.DE, "value": "Alter und Geschlecht"},
            {"language": TextLanguage.DE, "value": "Drogen"},
            {"language": TextLanguage.DE, "value": "Krankheiten allgemein"},
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [str(extracted_organization[0].stableTargetId)],
        # TODO(HS): add MergedOrganizationIdentifier of Robert Koch-Institut
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-2",
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "resourceTypeSpecific": [
            {
                "language": TextLanguage.EN,
                "value": "Monitoring-Studie",
            },
        ],
        "rights": [
            {
                "language": TextLanguage.DE,
                "value": "Lorem",
            },
        ],
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stableTargetId": Joker(),
        "temporal": "2000 - 2013",
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": TextLanguage.DE, "value": "Titel"}],
        "unitInCharge": [str(Identifier.generate(seed=234))],
        "wasGeneratedBy": str(extracted_activity.stableTargetId),
    }
    resources = transform_synopse_data_to_mex_resources(
        [synopse_studies[0]],
        [synopse_project],
        synopse_variables_by_study_id,
        [extracted_activity],
        extracted_primary_sources["report-server"],
        unit_merged_ids_by_synonym,
        extracted_organization[0],
        synopse_resource,
        MergedAccessPlatformIdentifier.generate(seed=236),
    )
    assert len(resources) == 1
    assert resources[0].model_dump(exclude_defaults=True) == expected_resource


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_projects_to_mex_activities(
    synopse_projects: list[SynopseProject],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_activity: ActivityMapping,
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> None:
    synopse_project = synopse_projects[0]
    contributor_merged_ids_by_name = {
        "Carla Contact": [MergedPersonIdentifier.generate(seed=12)]
    }
    unit_merged_ids_by_synonym = {
        "C1": MergedOrganizationalUnitIdentifier.generate(seed=13)
    }
    assert synopse_project.projektende
    assert synopse_project.projektbeginn
    expected_activity = {
        "abstract": [{"value": synopse_project.beschreibung_der_studie}],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": ["bFQoRhcVH5DHUD"],
        "documentation": [
            {
                "url": "file:///Z:/Projekte/Dokumentation",
                "title": "- Fragebogen\n- Labor",
            }
        ],
        "end": [str(TemporalEntity(synopse_project.projektende))],
        "externalAssociate": ["bWt8MuXvqsiYEDpjwYIT2S"],
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": synopse_project.studien_id,
        "involvedPerson": [str(Identifier.generate(seed=12))],
        "responsibleUnit": [str(Identifier.generate(seed=13))],
        "shortName": [{"value": "BBCCDD_00", "language": TextLanguage.DE}],
        "stableTargetId": Joker(),
        "start": [str(TemporalEntity(synopse_project.projektbeginn))],
        "succeeds": Joker(),
        "theme": ["https://mex.rki.de/item/theme-36"],
        "title": [{"language": TextLanguage.DE, "value": "Studie zu Lorem und Ipsum"}],
    }

    activities = list(
        transform_synopse_projects_to_mex_activities(
            synopse_projects,
            extracted_primary_sources["report-server"],
            contributor_merged_ids_by_name,
            unit_merged_ids_by_synonym,
            synopse_activity,
            synopse_organization_ids_by_query_string,
        )
    )

    assert len(activities) == 2
    assert activities[1][0].model_dump(exclude_defaults=True) == expected_activity
