from typing import Any

import pytest

from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.testing import Joker
from mex.common.types import (
    AccessRestriction,
    Identifier,
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TemporalEntity,
    Text,
    TextLanguage,
    Year,
)
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    split_off_extended_data_use_variables,
    transform_overviews_to_resource_lookup,
    transform_synopse_data_extended_data_use_to_mex_resources,
    transform_synopse_data_regular_to_mex_resources,
    transform_synopse_data_to_mex_resources,
    transform_synopse_projects_to_mex_activities,
    transform_synopse_studies_into_access_platforms,
    transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables,
    transform_synopse_variables_to_mex_variable_groups,
    transform_synopse_variables_to_mex_variables,
)


def test_split_off_extended_data_use_variables(
    synopse_variables: list[SynopseVariable],
    synopse_overviews: list[SynopseStudyOverview],
) -> None:
    expected_variable_count = len(synopse_variables)
    synopse_variables.append(
        SynopseVariable.model_validate(
            {  # variable not in synopse_overviews
                "textbox49": None,
                "Originalfrage": None,
                "StudieID1": "STUDY1",
                "StudieID2": 12345,
                "SymopseID": 12345678901111,
                "textbox51": None,
                "textbox5": "Gesundheiten (1101)",
                "textbox2": "Krankheiten allgemein (110100)",
                "valInstrument": None,
                "textbox21": "no auspraegung",
                "textbox24": "no_auspraegung",
                "IntVar": True,
                "KeepVarname": True,
            }
        )
    )
    expected_regular = {
        "auspraegungen": "-97",
        "datentyp": "Zahl",
        "int_var": False,
        "keep_varname": False,
        "studie_id": 12345,
        "studie": "STUDY1",
        "synopse_id": "1",
        "text_dt": "Nicht erhoben",
        "thema_und_fragebogenausschnitt": "Krankheiten (1101)",
        "unterthema": "Krankheiten allgemein (110100)",
        "val_instrument": "Health Questionnaire , Frage 18",
        "varlabel": "Angeborene Fehlbildung",
        "varname": "KHEfehlb",
    }
    expected_extended_data_use = {
        "int_var": True,
        "keep_varname": True,
        "studie_id": 12345,
        "studie": "STUDY1",
        "synopse_id": "12345678901111",
        "thema_und_fragebogenausschnitt": "Gesundheiten (1101)",
        "unterthema": "Krankheiten allgemein (110100)",
        "varlabel": "no auspraegung",
        "varname": "no_auspraegung",
    }
    (
        variables_regular,
        variables_extended_data_use,
    ) = split_off_extended_data_use_variables(
        synopse_variables,
        synopse_overviews,
    )
    list_regular = list(variables_regular)
    list_extended_data_use = list(variables_extended_data_use)
    assert len(list_regular) == expected_variable_count
    assert list_regular[0].model_dump(exclude_defaults=True) == expected_regular
    assert (
        list_extended_data_use[0].model_dump(exclude_defaults=True)
        == expected_extended_data_use
    )


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_studies_into_access_platforms(
    synopse_studies: list[SynopseStudy],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_access_platform: dict[str, Any],
) -> None:
    unit_merged_ids_by_synonym = {
        "C1": MergedOrganizationalUnitIdentifier.generate(seed=234)
    }
    expected_access_platform_one = {
        "contact": [Identifier.generate(seed=234)],
        "hadPrimarySource": extracted_primary_sources["report-server"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "Z:\\data",
        "landingPage": [{"url": "file:///Z:/data"}],
        "stableTargetId": Joker(),
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "Z:\\data"}],
        "unitInCharge": [Identifier.generate(seed=234)],
    }
    expected_access_platform_two = {
        "contact": [Identifier.generate(seed=234)],
        "hadPrimarySource": extracted_primary_sources["report-server"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "blabli blubb",
        "landingPage": [{"url": "blabli blubb"}],
        "stableTargetId": Joker(),
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "blabli blubb"}],
        "unitInCharge": [Identifier.generate(seed=234)],
    }

    access_platforms = list(
        transform_synopse_studies_into_access_platforms(
            synopse_studies,
            unit_merged_ids_by_synonym,
            extracted_primary_sources["report-server"],
            synopse_access_platform,
        )
    )
    assert len(access_platforms) == 4
    assert (
        access_platforms[0].model_dump(exclude_defaults=True)
        == expected_access_platform_one
    )
    assert (
        access_platforms[1].model_dump(exclude_defaults=True)
        == expected_access_platform_two
    )


def test_transform_overviews_to_resource_lookup() -> None:
    study_overviews = [
        SynopseStudyOverview(
            studien_id="studie1",
            ds_typ_id=17,
            titel_datenset="set1",
            synopse_id="synopse1",
        ),
        SynopseStudyOverview(
            studien_id="studie1",
            ds_typ_id=18,
            titel_datenset="set2",
            synopse_id="synopse1",
        ),
        SynopseStudyOverview(
            studien_id="studie2",
            ds_typ_id=32,
            titel_datenset="set2",
            synopse_id="synopse2",
        ),
    ]
    study_resources = [
        ExtractedResource(
            title="Found in overview",
            identifierInPrimarySource="studie1-17-set1",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
        ExtractedResource(
            title="Found in overview too",
            identifierInPrimarySource="studie1-18-set2",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
        ExtractedResource(
            title="Not found in overview",
            identifierInPrimarySource="not-found",
            hadPrimarySource=Identifier.generate(),
            accessRestriction=AccessRestriction["OPEN"],
            contact=[Identifier.generate()],
            unitInCharge=[Identifier.generate()],
            theme="https://mex.rki.de/item/theme-36",
        ),
    ]
    expected_lookup = {
        "synopse1": [
            study_resources[0].stableTargetId,
            study_resources[1].stableTargetId,
        ],
    }
    lookup = transform_overviews_to_resource_lookup(study_overviews, study_resources)
    assert lookup == expected_lookup


def test_transform_synopse_variables_to_mex_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    resource_ids_by_synopse_id: dict[str, Identifier],
) -> None:
    studie_id = 12345
    resource_id = resource_ids_by_synopse_id[str(studie_id)]
    expected_variable_group = {
        "containedBy": resource_id,
        "hadPrimarySource": extracted_primary_sources["report-server"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "Gesundheiten (1101)",
        "label": [{"language": TextLanguage.DE, "value": "Gesundheiten"}],
        "stableTargetId": Joker(),
    }

    variable_groups = list(
        transform_synopse_variables_to_mex_variable_groups(
            synopse_variables_by_thema,
            extracted_primary_sources["report-server"],
            resource_ids_by_synopse_id,
        )
    )
    assert (
        variable_groups[0].model_dump(exclude_defaults=True) == expected_variable_group
    )


def test_transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
    synopse_variables: list[SynopseVariable],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resource_ids_by_synopse_id: dict[str, list[Identifier]],
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
        "Krankheiten (1101)"
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
        "label": [dict(language=TextLanguage("de"), value="Angeborene Fehlbildung")],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["1"]],
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
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["2"]],
        "identifierInPrimarySource": "2",
        "valueSet": ["Ja"],
    }
    expected_variable_three = {  # var 3, no auspraegung
        "belongsTo": [str(variable_group.stableTargetId)],
        "dataType": "Text",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "label": [{"value": "no auspraegung"}],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["3"]],
        "identifierInPrimarySource": "3",
    }
    expected_variable_four = {  # var 4, different value in textbox5
        "belongsTo": [str(variable_group.stableTargetId)],
        "dataType": "Text",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "label": [{"value": "no auspraegung"}],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["5"]],
        "identifierInPrimarySource": "5",
    }
    variables = list(
        transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
            synopse_variables,
            variable_group,
            resource_ids_by_synopse_id,
            extracted_primary_sources["report-server"],
        )
    )
    assert len(variables) == 4
    assert variables[0].model_dump(exclude_defaults=True) == expected_variable_one
    assert variables[1].model_dump(exclude_defaults=True) == expected_variable_two
    assert variables[2].model_dump(exclude_defaults=True) == expected_variable_three
    assert variables[3].model_dump(exclude_defaults=True) == expected_variable_four


def test_transform_synopse_variables_to_mex_variables(
    synopse_variables_by_thema: dict[int, list[SynopseVariable]],
    extracted_variable_groups: list[ExtractedVariableGroup],
    resource_ids_by_synopse_id: dict[str, list[Identifier]],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group for group in extracted_variable_groups
    }
    variables = list(
        transform_synopse_variables_to_mex_variables(
            synopse_variables_by_thema,
            extracted_variable_groups,
            resource_ids_by_synopse_id,
            extracted_primary_sources["report-server"],
        )
    )

    assert len(variables) == 5
    assert variables[0].model_dump(exclude_defaults=True) == {
        "belongsTo": [
            str(
                variable_group_by_identifier_in_primary_source[
                    "Gesundheiten (1101)"
                ].stableTargetId
            )
        ],
        "dataType": "Zahl",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "label": [{"value": "no auspraegung"}],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["4"]],
        "identifierInPrimarySource": "4",
    }
    assert variables[1].model_dump(exclude_defaults=True) == {
        "belongsTo": [
            str(
                variable_group_by_identifier_in_primary_source[
                    "Krankheiten (1101)"
                ].stableTargetId
            )
        ],
        "codingSystem": "Health Questionnaire , Frage 18",
        "dataType": "Zahl",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "1",
        "label": [dict(language=TextLanguage("de"), value="Angeborene Fehlbildung")],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["1"]],
        "valueSet": ["Nicht erhoben", "Weiß nicht"],
    }
    assert variables[4].model_dump(exclude_defaults=True) == {
        "belongsTo": [
            str(
                variable_group_by_identifier_in_primary_source[
                    "Krankheiten (1101)"
                ].stableTargetId
            )
        ],
        "dataType": "Text",
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "identifier": Joker(),
        "label": [{"value": "no auspraegung"}],
        "stableTargetId": Joker(),
        "usedIn": [str(rid) for rid in resource_ids_by_synopse_id["5"]],
        "identifierInPrimarySource": "5",
    }


def test_transform_synopse_data_to_mex_resources(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_project: SynopseProject,
    synopse_studies: list[SynopseStudy],
    extracted_activity: ExtractedActivity,
    extracted_access_platforms: list[ExtractedAccessPlatform],
    created_by_study_id: dict[str, str | None],
    description_by_study_id: dict[str, str | None],
    documentation_by_study_id: dict[str, Link | None],
    keyword_text_by_study_id: dict[str, list[Text]],
    extracted_organization: list[ExtractedOrganization],
    synopse_resource_extended_data_use: dict[str, Any],
) -> None:
    unit_merged_ids_by_synonym = {"C1": Identifier.generate(seed=234)}
    access_platform_by_identifier_in_primary_source = {
        p.identifierInPrimarySource: p for p in extracted_access_platforms
    }
    identifier_in_primary_source_by_studien_id = {
        synopse_studies[
            0
        ].studien_id: f"{synopse_studies[0].studien_id}-{synopse_studies[0].ds_typ_id}-"
        f"{synopse_studies[0].titel_datenset}"
    }
    expected_resource = {
        "accessPlatform": [
            str(
                access_platform_by_identifier_in_primary_source[
                    synopse_studies[0].plattform_adresse
                ].stableTargetId
            )
        ],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": [str(Identifier.generate(seed=234))],
        "contributingUnit": extracted_activity.involvedUnit
        + extracted_activity.responsibleUnit,
        "contributor": extracted_activity.involvedPerson,
        "created": Year(synopse_studies[0].erstellungs_datum),
        "description": [
            {"language": TextLanguage.DE, "value": synopse_studies[0].beschreibung}
        ],
        "documentation": [{"url": "Z:\\foo\\bar"}],
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Niemand darf irgendwas.",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifier": Joker(),
        "identifierInPrimarySource": (
            identifier_in_primary_source_by_studien_id[synopse_studies[0].studien_id]
        ),
        "keyword": [
            {
                "language": TextLanguage.DE,
                "value": "Alkohol, Alter und Geschlecht, Drogen",
            }
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [str(extracted_organization[0].stableTargetId)],
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
        "rights": [{"language": TextLanguage.DE, "value": "Gesundheitsdaten"}],
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stableTargetId": Joker(),
        "temporal": "2000 - 2013",
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [
            {"language": TextLanguage.DE, "value": synopse_studies[0].titel_datenset}
        ],
        "unitInCharge": [str(Identifier.generate(seed=234))],
        "wasGeneratedBy": str(extracted_activity.stableTargetId),
    }

    resources = list(
        transform_synopse_data_to_mex_resources(
            [synopse_studies[0]],
            [synopse_project],
            [extracted_activity],
            extracted_access_platforms,
            extracted_primary_sources["report-server"],
            unit_merged_ids_by_synonym,
            extracted_organization[0],
            created_by_study_id,
            description_by_study_id,
            documentation_by_study_id,
            keyword_text_by_study_id,
            synopse_resource_extended_data_use,
            identifier_in_primary_source_by_studien_id,
            {
                synopse_studies[0].studien_id: Text(
                    language=TextLanguage.DE, value=synopse_studies[0].titel_datenset
                )
            },
        )
    )
    assert len(resources) == 1
    assert resources[0].model_dump(exclude_defaults=True) == expected_resource


def test_transform_synopse_data_regular_to_mex_resources(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_project: SynopseProject,
    synopse_studies: list[SynopseStudy],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    extracted_activity: ExtractedActivity,
    extracted_access_platforms: list[ExtractedAccessPlatform],
    extracted_organization: list[ExtractedOrganization],
    synopse_resource_extended_data_use: dict[str, Any],
) -> None:
    unit_merged_ids_by_synonym = {"C1": Identifier.generate(seed=234)}
    access_platform_by_identifier_in_primary_source = {
        p.identifierInPrimarySource: p for p in extracted_access_platforms
    }
    expected_resource = {
        "accessPlatform": [
            str(
                access_platform_by_identifier_in_primary_source[
                    synopse_studies[0].plattform_adresse
                ].stableTargetId
            )
        ],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": [str(Identifier.generate(seed=234))],
        "contributingUnit": extracted_activity.involvedUnit
        + extracted_activity.responsibleUnit,
        "contributor": extracted_activity.involvedPerson,
        "created": Year("2022"),
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
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifier": Joker(),
        "identifierInPrimarySource": ("12345-17-Titel"),
        "keyword": [
            {"language": TextLanguage.DE, "value": "Alkohol"},
            {"language": TextLanguage.DE, "value": "Alter und Geschlecht"},
            {"language": TextLanguage.DE, "value": "Drogen"},
            {"language": TextLanguage.DE, "value": "Krankheiten allgemein"},
        ],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [str(extracted_organization[0].stableTargetId)],
        # TODO add MergedOrganizationIdentifier of Robert Koch-Institut
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
                "value": "Gesundheitsdaten",
                "language": TextLanguage.DE,
            }
        ],
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stableTargetId": Joker(),
        "temporal": "2000 - 2013",
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": TextLanguage.DE, "value": "Titel"}],
        "unitInCharge": [str(Identifier.generate(seed=234))],
        "wasGeneratedBy": str(extracted_activity.stableTargetId),
    }
    resources = list(
        transform_synopse_data_regular_to_mex_resources(
            [synopse_studies[0]],
            [synopse_project],
            synopse_variables_by_study_id,
            [extracted_activity],
            extracted_access_platforms,
            extracted_primary_sources["report-server"],
            unit_merged_ids_by_synonym,
            extracted_organization[0],
            synopse_resource_extended_data_use,
        )
    )
    assert len(resources) == 1
    assert resources[0].model_dump(exclude_defaults=True) == expected_resource


def test_transform_synopse_data_extended_data_use_to_mex_resources(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    synopse_project: SynopseProject,
    synopse_studies: list[SynopseStudy],
    synopse_variables_extended_data_use_by_study_id: dict[int, list[SynopseVariable]],
    extracted_activity: ExtractedActivity,
    extracted_access_platforms: list[ExtractedAccessPlatform],
    extracted_organization: list[ExtractedOrganization],
    synopse_resource: dict[str, Any],
) -> None:
    unit_merged_ids_by_synonym = {"C1": Identifier.generate(seed=234)}
    access_platform_by_identifier_in_primary_source = {
        p.identifierInPrimarySource: p for p in extracted_access_platforms
    }
    expected_resource = {
        "accessPlatform": [
            str(
                access_platform_by_identifier_in_primary_source[
                    synopse_studies[0].plattform_adresse
                ].stableTargetId
            )
        ],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "contact": [str(Identifier.generate(seed=234))],
        "contributingUnit": extracted_activity.involvedUnit
        + extracted_activity.responsibleUnit,
        "contributor": extracted_activity.involvedPerson,
        "hadPrimarySource": str(
            extracted_primary_sources["report-server"].stableTargetId
        ),
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "Niemand darf irgendwas.",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifier": Joker(),
        "identifierInPrimarySource": ("12345-extended-data-use"),
        "keyword": [{"language": TextLanguage.DE, "value": "Krankheiten allgemein"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": [str(extracted_organization[0].stableTargetId)],
        # TODO add MergedOrganizationIdentifier of Robert Koch-Institut
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
                "value": "Gesundheitsdaten",
                "language": TextLanguage.DE,
            }
        ],
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stableTargetId": Joker(),
        "temporal": "2000 - 2013",
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": "de", "value": "Studie123: Erweiterte Datennutzung"}],
        "unitInCharge": [str(Identifier.generate(seed=234))],
        "wasGeneratedBy": str(extracted_activity.stableTargetId),
    }
    resources = list(
        transform_synopse_data_extended_data_use_to_mex_resources(
            [synopse_studies[0]],
            [synopse_project],
            synopse_variables_extended_data_use_by_study_id,
            [extracted_activity],
            extracted_access_platforms,
            extracted_primary_sources["report-server"],
            unit_merged_ids_by_synonym,
            extracted_organization[0],
            synopse_resource,
        )
    )
    assert len(resources) == 1
    assert resources[0].model_dump(exclude_defaults=True) == expected_resource


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_synopse_projects_to_mex_activities(
    synopse_projects: list[SynopseProject],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_person: ExtractedPerson,
    synopse_activity: dict[str, Any],
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> None:
    synopse_project = synopse_projects[0]
    contact_merged_ids_by_emails = {"info@rki.de": extracted_person.stableTargetId}
    contributor_merged_ids_by_name = {"Carla Contact": Identifier.generate(seed=12)}
    unit_merged_ids_by_synonym = {"FG 99": Identifier.generate(seed=13)}

    expected_activity = {
        "abstract": [{"value": synopse_project.beschreibung_der_studie}],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": [extracted_person.stableTargetId],
        "documentation": [
            {
                "url": "file:///Z:/Projekte/Dokumentation",
                "title": "- Fragebogen\n- Labor",
            }
        ],
        "end": [TemporalEntity(synopse_project.projektende)],
        "hadPrimarySource": extracted_primary_sources["report-server"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": synopse_project.studien_id,
        "involvedPerson": [Identifier.generate(seed=12)],
        "responsibleUnit": [Identifier.generate(seed=13)],
        "shortName": [{"value": "BBCCDD_00"}],
        "stableTargetId": Joker(),
        "start": [TemporalEntity(synopse_project.projektbeginn)],
        "succeeds": Joker(),
        "theme": ["https://mex.rki.de/item/theme-36"],
        "title": [{"language": TextLanguage.DE, "value": "Studie zu Lorem und Ipsum"}],
    }

    activities = list(
        transform_synopse_projects_to_mex_activities(
            synopse_projects,
            extracted_primary_sources["report-server"],
            contact_merged_ids_by_emails,
            contributor_merged_ids_by_name,
            unit_merged_ids_by_synonym,
            synopse_activity,
            synopse_organization_ids_by_query_string,
        )
    )

    assert len(activities) == 2
    assert activities[0].model_dump(exclude_defaults=True) == expected_activity
