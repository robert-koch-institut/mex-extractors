from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Email,
    LinkLanguage,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    MergedPrimarySourceIdentifier,
    TextLanguage,
    YearMonthDay,
)
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection
from mex.extractors.sumo.transform import (
    get_contact_merged_ids_by_emails,
    get_contact_merged_ids_by_names,
    transform_feat_projection_variable_to_mex_variable,
    transform_feat_variable_to_mex_variable_group,
    transform_model_nokeda_variable_to_mex_variable_group,
    transform_nokeda_aux_variable_to_mex_variable,
    transform_nokeda_aux_variable_to_mex_variable_group,
    transform_nokeda_model_variable_to_mex_variable,
    transform_resource_feat_model_to_mex_resource,
    transform_resource_nokeda_to_mex_resource,
    transform_sumo_access_platform_to_mex_access_platform,
    transform_sumo_activity_to_extracted_activity,
)


def test_get_contact_merged_ids_by_emails(
    mex_actor_resources: ExtractedContactPoint,
) -> None:
    contact_merged_ids_by_emails = get_contact_merged_ids_by_emails(
        [mex_actor_resources]
    )
    assert contact_merged_ids_by_emails == {
        "email@email.de": mex_actor_resources.stableTargetId
    }


def test_get_contact_merged_ids_by_names(
    mex_actor_access_platform: ExtractedPerson,
) -> None:
    contact_merged_ids_by_names = get_contact_merged_ids_by_names(
        [mex_actor_access_platform]
    )
    assert contact_merged_ids_by_names == {
        "Erika Mustermann": mex_actor_access_platform.stableTargetId
    }


def test_transform_resource_nokeda_to_mex_resource(  # noqa: PLR0913
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    sumo_resources_nokeda: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    transformed_activity: ExtractedActivity,
    sumo_extracted_access_platform: ExtractedAccessPlatform,
) -> None:
    contact_merged_ids_by_emails = {
        Email("email@email.de"): MergedContactPointIdentifier.generate(43)
    }
    mex_source = transform_resource_nokeda_to_mex_resource(
        sumo_resources_nokeda,
        extracted_primary_sources["nokeda"],
        unit_merged_ids_by_synonym,
        contact_merged_ids_by_emails,
        extracted_organization_rki,
        transformed_activity,
        sumo_extracted_access_platform,
    )
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": MergedPrimarySourceIdentifier(
            extracted_primary_sources["nokeda"].stableTargetId
        ),
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifierInPrimarySource": "test_project",
        "stableTargetId": Joker(),
        "accessPlatform": [sumo_extracted_access_platform.stableTargetId],
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "contact": [MergedPersonIdentifier.generate(43)],
        "contributingUnit": [Joker()],
        "description": [
            {
                "language": TextLanguage.DE,
                "value": "Echtzeitdaten der Routinedokumenation",
            }
        ],
        "documentation": [
            {
                "language": LinkLanguage.EN,
                "title": "Confluence",
                "url": "https://link.com",
            }
        ],
        "externalPartner": Joker(),
        "keyword": [
            {"language": TextLanguage.DE, "value": "keyword1"},
            {
                "language": TextLanguage.DE,
                "value": "keyword2",
            },
        ],
        "meshId": ["http://id.nlm.nih.gov/mesh/D004636"],
        "publisher": [extracted_organization_rki.stableTargetId],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-3",
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "resourceTypeSpecific": [{"language": TextLanguage.DE, "value": "Daten"}],
        "rights": [
            {
                "language": TextLanguage.DE,
                "value": "Die Daten sind zweckgebunden und können nicht ohne Weiteres innerhalb des RKI zur Nutzung zur Verfügung gestellt werden.",
            }
        ],
        "spatial": [{"language": TextLanguage.DE, "value": "Deutschland"}],
        "stateOfDataProcessing": ["https://mex.rki.de/item/data-processing-state-2"],
        "theme": [
            "https://mex.rki.de/item/theme-11",
        ],
        "title": [{"language": TextLanguage.DE, "value": "test_project"}],
        "unitInCharge": [unit_merged_ids_by_synonym["FG99"]],
        "wasGeneratedBy": transformed_activity.stableTargetId,
    }
    assert mex_source.model_dump(exclude_defaults=True) == expected


def test_transform_resource_feat_model_to_mex_resource(  # noqa: PLR0913
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    sumo_resources_feat: ResourceMapping,
    mex_resources_nokeda: ExtractedResource,
    transformed_activity: ExtractedActivity,
    sumo_extracted_access_platform: ExtractedAccessPlatform,
) -> None:
    contact_merged_ids_by_emails = {
        Email("email@email.de"): MergedContactPointIdentifier.generate(43)
    }
    mex_source = transform_resource_feat_model_to_mex_resource(
        sumo_resources_feat,
        extracted_primary_sources["nokeda"],
        unit_merged_ids_by_synonym,
        contact_merged_ids_by_emails,
        mex_resources_nokeda,
        transformed_activity,
        sumo_extracted_access_platform,
    )
    expected = {
        "accessPlatform": [sumo_extracted_access_platform.stableTargetId],
        "identifier": Joker(),
        "hadPrimarySource": MergedPrimarySourceIdentifier(
            extracted_primary_sources["nokeda"].stableTargetId
        ),
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifierInPrimarySource": "Syndrome",
        "stableTargetId": Joker(),
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-17",
        "contact": [MergedContactPointIdentifier.generate(43)],
        "contributingUnit": [Joker()],
        "isPartOf": [mex_resources_nokeda.stableTargetId],
        "keyword": [
            {"language": TextLanguage.DE, "value": "keyword 1"},
            {"language": TextLanguage.DE, "value": "keyword 2"},
        ],
        "meshId": ["http://id.nlm.nih.gov/mesh/D004636"],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-1",
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "resourceTypeSpecific": [
            {
                "language": TextLanguage.DE,
                "value": "Dummy resource type",
            },
        ],
        "spatial": [
            {
                "language": TextLanguage.DE,
                "value": "Dummy spatial",
            },
        ],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": TextLanguage.DE, "value": "Syndrome"}],
        "unitInCharge": [unit_merged_ids_by_synonym["FG 99"]],
        "wasGeneratedBy": transformed_activity.stableTargetId,
    }
    assert mex_source.model_dump(exclude_defaults=True) == expected


def test_transform_nokeda_aux_variable_to_mex_variable_group(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_nokeda: ExtractedResource,
    cc2_aux_model: list[Cc2AuxModel],
) -> None:
    expected = {
        "containedBy": [mex_resources_nokeda.stableTargetId],
        "hadPrimarySource": MergedPrimarySourceIdentifier(
            extracted_primary_sources["nokeda"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "age",
        "label": [{"language": TextLanguage.EN, "value": "age"}],
        "stableTargetId": Joker(),
    }
    transformed_data = list(
        transform_nokeda_aux_variable_to_mex_variable_group(
            cc2_aux_model,
            extracted_primary_sources["nokeda"],
            mex_resources_nokeda,
        )
    )
    assert len(transformed_data) == 2
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected


def test_transform_model_nokeda_variable_to_mex_variable_group(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_nokeda: ExtractedResource,
    cc1_data_model_nokeda: list[Cc1DataModelNoKeda],
) -> None:
    expected = {
        "containedBy": [mex_resources_nokeda.stableTargetId],
        "hadPrimarySource": MergedPrimarySourceIdentifier(
            extracted_primary_sources["nokeda"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "data supply",
        "label": [
            {"language": TextLanguage.DE, "value": "Datenbereitstellung"},
            {"language": TextLanguage.EN, "value": "data supply"},
        ],
        "stableTargetId": Joker(),
    }
    transformed_data = list(
        transform_model_nokeda_variable_to_mex_variable_group(
            cc1_data_model_nokeda,
            extracted_primary_sources["nokeda"],
            mex_resources_nokeda,
        )
    )
    assert len(transformed_data) == 1
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected


def test_transform_feat_variable_to_mex_variable_group(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_nokeda: ExtractedResource,
    cc2_feat_projection: list[Cc2FeatProjection],
) -> None:
    expected = {
        "containedBy": [mex_resources_nokeda.stableTargetId],
        "hadPrimarySource": MergedPrimarySourceIdentifier(
            extracted_primary_sources["nokeda"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "feat_syndrome RSV",
        "label": [{"value": "feat_syndrome RSV"}],
        "stableTargetId": Joker(),
    }
    transformed_data = list(
        transform_feat_variable_to_mex_variable_group(
            cc2_feat_projection,
            extracted_primary_sources["nokeda"],
            mex_resources_nokeda,
        )
    )
    assert len(transformed_data) == 1
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected


def test_transform_nokeda_model_variable_to_mex_variable(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_nokeda: ExtractedResource,
    mex_variable_groups_model_nokeda: list[ExtractedVariableGroup],
    cc1_data_model_nokeda: list[Cc1DataModelNoKeda],
    cc1_data_valuesets: list[Cc1DataValuesets],
) -> None:
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_model_nokeda
        for label in m.label
        if label.language == TextLanguage.DE
    }
    variable = cc1_data_model_nokeda[0]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "nokeda_edis_software",
        "stableTargetId": Joker(),
        "belongsTo": [stable_target_id_by_label_values[variable.domain]],
        "dataType": "string",
        "description": [
            {"language": TextLanguage.DE, "value": "shobidoo"},
            {"language": TextLanguage.EN, "value": "shobidoo_en"},
        ],
        "label": [
            {"value": "Name des EDIS", "language": TextLanguage.DE},
            {"value": "Name of EDIS", "language": TextLanguage.EN},
        ],
        "usedIn": [mex_resources_nokeda.stableTargetId],
    }
    transformed_data = list(
        transform_nokeda_model_variable_to_mex_variable(
            cc1_data_model_nokeda,
            cc1_data_valuesets,
            mex_variable_groups_model_nokeda,
            mex_resources_nokeda,
            extracted_primary_sources["nokeda"],
        )
    )
    assert len(transformed_data) == 1
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected


def test_transform_nokeda_aux_variable_to_mex_variable(  # noqa: PLR0913
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_nokeda: ExtractedResource,
    mex_variable_groups_nokeda_aux: list[ExtractedVariableGroup],
    cc2_aux_model: list[Cc2AuxModel],
    cc2_aux_mapping: list[Cc2AuxMapping],
    cc2_aux_valuesets: list[Cc2AuxValuesets],
) -> None:
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_nokeda_aux
        for label in list(m.label)
        if label.language == TextLanguage.EN
    }
    transformed_data = list(
        transform_nokeda_aux_variable_to_mex_variable(
            cc2_aux_model,
            cc2_aux_mapping,
            cc2_aux_valuesets,
            mex_variable_groups_nokeda_aux,
            mex_resources_nokeda,
            extracted_primary_sources["nokeda"],
        )
    )
    assert len(transformed_data) == 2
    variable = cc2_aux_model[0]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "aux_age21_min",
        "stableTargetId": Joker(),
        "belongsTo": [stable_target_id_by_label_values[variable.domain]],
        "description": [
            {"language": TextLanguage.EN, "value": "the lowest age in the age group"}
        ],
        "label": [{"language": "fr", "value": "aux_age21_min"}],
        "usedIn": [mex_resources_nokeda.stableTargetId],
        "valueSet": Joker(),
    }
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected
    variable = list(cc2_aux_model)[1]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "aux_cedis_group",
        "stableTargetId": Joker(),
        "belongsTo": [stable_target_id_by_label_values[variable.domain]],
        "description": [
            {
                "language": TextLanguage.EN,
                "value": "Core groups as defined in the CEDIS reporting standard",
            }
        ],
        "label": [{"language": "fr", "value": "aux_cedis_group"}],
        "usedIn": [mex_resources_nokeda.stableTargetId],
        "valueSet": Joker(),
    }
    assert transformed_data[1].model_dump(exclude_defaults=True) == expected


def test_transform_feat_projection_variable_to_mex_variable(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mex_resources_feat: ExtractedResource,
    mex_variable_groups_model_feat: list[ExtractedVariableGroup],
    cc2_feat_projection: list[Cc2FeatProjection],
) -> None:
    stable_target_id_by_label_values = {
        m.label[0].value: m.stableTargetId for m in mex_variable_groups_model_feat
    }
    variable = cc2_feat_projection[0]
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "feat_syndrome RSV 1",
        "stableTargetId": Joker(),
        "belongsTo": [
            stable_target_id_by_label_values[
                f"{variable.feature_domain} {variable.feature_subdomain}" or ""
            ]
        ],
        "description": [{"value": "specific RSV-ICD-10 codes"}],
        "label": [
            {
                "language": TextLanguage.DE,
                "value": "Respiratorisches Syncytial-Virus, spezifisch",
            },
            {
                "language": TextLanguage.EN,
                "value": "respiratory syncytial virus, specific",
            },
        ],
        "usedIn": [mex_resources_feat.stableTargetId],
    }

    transformed_data = list(
        transform_feat_projection_variable_to_mex_variable(
            cc2_feat_projection,
            mex_variable_groups_model_feat,
            mex_resources_feat,
            extracted_primary_sources["nokeda"],
        )
    )
    assert len(transformed_data) == 1
    assert transformed_data[0].model_dump(exclude_defaults=True) == expected


def test_transform_sumo_access_platform_to_mex_access_platform(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    sumo_access_platform: AccessPlatformMapping,
) -> None:
    person_stable_target_ids_by_query_string = {
        "Roland Resolved": MergedPersonIdentifier.generate(seed=30)
    }
    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "sumo-db",
        "stableTargetId": Joker(),
        "contact": [person_stable_target_ids_by_query_string["Roland Resolved"]],
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "SUMO Datenbank", "language": TextLanguage.DE}],
        "unitInCharge": [unit_merged_ids_by_synonym["MF4"]],
    }

    transformed_data = transform_sumo_access_platform_to_mex_access_platform(
        sumo_access_platform,
        unit_merged_ids_by_synonym,
        person_stable_target_ids_by_query_string,
        extracted_primary_sources["nokeda"],
    )

    assert transformed_data.model_dump(exclude_defaults=True) == expected


def test_transform_sumo_activity_to_extracted_activity(
    sumo_activity: ActivityMapping,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    contact_merged_ids_by_emails: dict[Email, MergedContactPointIdentifier],
) -> None:
    extracted_activity = transform_sumo_activity_to_extracted_activity(
        sumo_activity,
        unit_merged_ids_by_synonym,
        contact_merged_ids_by_emails,
        extracted_primary_sources["nokeda"],
    )

    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources["nokeda"].stableTargetId,
        "identifierInPrimarySource": "https://url.url",
        "stableTargetId": Joker(),
        "abstract": [{"value": "Dummy abstract", "language": TextLanguage.DE}],
        "activityType": ["https://mex.rki.de/item/activity-type-3"],
        "contact": [contact_merged_ids_by_emails[Email("email@email.de")]],
        "documentation": [
            {
                "language": LinkLanguage.DE,
                "title": "SUMO im internen RKI Confluence",
                "url": "https://url.url",
            }
        ],
        "externalAssociate": Joker(),
        "involvedUnit": [unit_merged_ids_by_synonym["MF4"]],
        "responsibleUnit": [unit_merged_ids_by_synonym["FG32"]],
        "shortName": [{"value": "SUMO", "language": TextLanguage.DE}],
        "start": [YearMonthDay("2018-07-01")],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-36",
        ],
        "title": [
            {"value": "SUMO Notaufnahmesurveillance", "language": TextLanguage.DE}
        ],
        "website": [
            {
                "language": LinkLanguage.DE,
                "title": "Surveillance Monitor",
                "url": "https://url.url",
            }
        ],
    }
    assert extracted_activity.model_dump(exclude_defaults=True) == expected
