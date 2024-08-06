from collections import defaultdict
from collections.abc import Generator, Iterable
from typing import Any

from mex.common.logging import watch
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.types import (
    Email,
    Link,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
)
from mex.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.sumo.models.cc2_feat_projection import Cc2FeatProjection

VARIABLE_DATA_TYPE_MAP = defaultdict(
    lambda: "https://mex.rki.de/item/data-type-2",
    {
        "number": "https://mex.rki.de/item/data-type-1",
        "string": "https://mex.rki.de/item/data-type-2",
        "boolean": "https://mex.rki.de/item/data-type-3",
    },
)


def get_contact_merged_ids_by_emails(
    mex_actor_resources: Iterable[ExtractedContactPoint],
) -> dict[Email, MergedContactPointIdentifier]:
    """Get merged id by emails lookup.

    Args:
        mex_actor_resources: Iterable of extracted contact points

    Returns:
        dict of contact merged ids by email
    """
    return {
        Email(email.lower()): MergedContactPointIdentifier(actor.stableTargetId)
        for actor in mex_actor_resources
        for email in actor.email
    }


def get_contact_merged_ids_by_names(
    mex_actors_access_platform: Iterable[ExtractedPerson],
) -> dict[str, MergedPersonIdentifier]:
    """Get merged id by name lookup.

    Args:
        mex_actors_access_platform: Iterable of extracted persons

    Returns:
        dict of contact merged ids by name
    """
    return {
        f"{', '.join(a.givenName)} {', '.join(a.familyName)}": a.stableTargetId
        for a in mex_actors_access_platform
    }


def transform_resource_feat_model_to_mex_resource(
    extracted_sumo_resource_feat: dict[str, Any],
    extracted_primary_source: ExtractedPrimarySource,
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    contact_merged_ids_by_emails: dict[Email, MergedContactPointIdentifier],
    mex_resource_nokeda: ExtractedResource,
    transformed_activity: ExtractedActivity,
) -> ExtractedResource:
    """Transform extracted_sumo_resource_feat to ExtractedResource.

    Args:
        extracted_sumo_resource_feat: extracted_sumo_resource_feat default values
        extracted_primary_source: Extracted primary source
        unit_merged_ids_by_synonym: Mapping from synonyms to merged IDs of units
        contact_merged_ids_by_emails: Mapping from emails to merged IDs of contact
            points
        mex_resource_nokeda: ExtractedResource for nokeda
        transformed_activity: ExtractedActivity for sumo

    Returns:
        ExtractedResource
    """
    keyword = [
        k["setValues"][0]
        for k in extracted_sumo_resource_feat["keyword"][0]["mappingRules"]
    ]
    return ExtractedResource(
        accessRestriction=extracted_sumo_resource_feat["accessRestriction"][0][
            "mappingRules"
        ][0]["setValues"][0],
        accrualPeriodicity=extracted_sumo_resource_feat["accrualPeriodicity"][0][
            "mappingRules"
        ][0]["setValues"][0],
        contact=contact_merged_ids_by_emails[
            extracted_sumo_resource_feat["contact"][0]["mappingRules"][0]["forValues"][
                0
            ]
        ],
        contributingUnit=unit_merged_ids_by_synonym[
            extracted_sumo_resource_feat["contributingUnit"][0]["mappingRules"][0][
                "forValues"
            ][0]
        ],
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=extracted_sumo_resource_feat["title"][0][
            "mappingRules"
        ][0]["setValues"][0]["value"],
        isPartOf=mex_resource_nokeda.stableTargetId,
        keyword=keyword,
        meshId=extracted_sumo_resource_feat["meshId"][0]["mappingRules"][0][
            "setValues"
        ][0],
        resourceTypeGeneral=extracted_sumo_resource_feat["resourceTypeGeneral"][0][
            "mappingRules"
        ][0]["setValues"],
        theme=extracted_sumo_resource_feat["theme"][0]["mappingRules"][0]["setValues"],
        title=extracted_sumo_resource_feat["title"][0]["mappingRules"][0]["setValues"][
            0
        ],
        unitInCharge=unit_merged_ids_by_synonym[
            extracted_sumo_resource_feat["unitInCharge"][0]["mappingRules"][0][
                "forValues"
            ][0]
        ],
        wasGeneratedBy=transformed_activity.stableTargetId,
    )


def transform_resource_nokeda_to_mex_resource(
    extracted_sumo_resource_nokeda: dict[str, Any],
    extracted_primary_source: ExtractedPrimarySource,
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    contact_merged_ids_by_emails: dict[Email, MergedContactPointIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    transformed_activity: ExtractedActivity,
) -> ExtractedResource:
    """Transform ResourceNokeda to ExtractedResource.

    Args:
        extracted_sumo_resource_nokeda: extracted_sumo_resource_nokeda default values
        extracted_primary_source: Extracted primary source
        unit_merged_ids_by_synonym: Mapping from synonyms to merged IDs of units
        contact_merged_ids_by_emails: Mapping from emails to merged IDs of contact
                                      points
        extracted_organization_rki: ExtractedOrganization
        transformed_activity: ExtractedActivity for sumo

    Returns:
        ExtractedResource
    """
    keyword = [
        k["setValues"][0]
        for k in extracted_sumo_resource_nokeda["keyword"][0]["mappingRules"]
    ]

    return ExtractedResource(
        accessRestriction=extracted_sumo_resource_nokeda["accessRestriction"][0][
            "mappingRules"
        ][0]["setValues"][0],
        accrualPeriodicity=extracted_sumo_resource_nokeda["accrualPeriodicity"][0][
            "mappingRules"
        ][0]["setValues"][0],
        contact=contact_merged_ids_by_emails[
            extracted_sumo_resource_nokeda["contact"][0]["mappingRules"][0][
                "forValues"
            ][0]
        ],
        contributingUnit=unit_merged_ids_by_synonym[
            extracted_sumo_resource_nokeda["contributingUnit"][0]["mappingRules"][0][
                "forValues"
            ][0]
        ],
        description=Text.model_validate(
            extracted_sumo_resource_nokeda["description"][0]["mappingRules"][0][
                "setValues"
            ][0]
        ),
        documentation=Link.model_validate(
            extracted_sumo_resource_nokeda["documentation"][0]["mappingRules"][0][
                "setValues"
            ][0]
        ),
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=extracted_sumo_resource_nokeda["title"][0][
            "mappingRules"
        ][0]["setValues"][0]["value"],
        keyword=keyword,
        meshId=[
            extracted_sumo_resource_nokeda["meshId"][0]["mappingRules"][0]["setValues"][
                0
            ]
        ],
        publication=Link.model_validate(
            extracted_sumo_resource_nokeda["publication"][0]["mappingRules"][0][
                "setValues"
            ][0]
        ),
        publisher=extracted_organization_rki.stableTargetId,
        resourceTypeGeneral=extracted_sumo_resource_nokeda["resourceTypeGeneral"][0][
            "mappingRules"
        ][0]["setValues"][0],
        resourceTypeSpecific=extracted_sumo_resource_nokeda["resourceTypeSpecific"][0][
            "mappingRules"
        ][0]["setValues"][0],
        rights=Text.model_validate(
            extracted_sumo_resource_nokeda["rights"][0]["mappingRules"][0]["setValues"][
                0
            ]
        ),
        spatial=extracted_sumo_resource_nokeda["spatial"][0]["mappingRules"][0][
            "setValues"
        ][0],
        stateOfDataProcessing=extracted_sumo_resource_nokeda["stateOfDataProcessing"][
            0
        ]["mappingRules"][0]["setValues"][0],
        theme=[
            k["setValues"][0]
            for k in extracted_sumo_resource_nokeda["theme"][0]["mappingRules"]
        ],
        title=extracted_sumo_resource_nokeda["title"][0]["mappingRules"][0][
            "setValues"
        ][0],
        unitInCharge=unit_merged_ids_by_synonym[
            extracted_sumo_resource_nokeda["unitInCharge"][0]["mappingRules"][0][
                "forValues"
            ][0]
        ],
        wasGeneratedBy=transformed_activity.stableTargetId,
    )


@watch
def transform_nokeda_aux_variable_to_mex_variable_group(
    extracted_cc2_aux_model: Iterable[Cc2AuxModel],
    extracted_primary_source: ExtractedPrimarySource,
    mex_resource_nokeda: ExtractedResource,
) -> Generator[ExtractedVariableGroup, None, None]:
    """Transform nokeda aux variables to ExtractedVariableGroups.

    Args:
        extracted_cc2_aux_model: Cc2AuxModel variables
        extracted_primary_source: Extracted primary source
        mex_resource_nokeda: ExtractedResource for nokeda

    Returns:
        Generator for ExtractedVariableGroup
    """
    contained_by = mex_resource_nokeda.stableTargetId
    used_identifier_in_primary_source = set()
    for variable in extracted_cc2_aux_model:
        identifier_in_primary_source = variable.domain
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            yield ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                identifierInPrimarySource=identifier_in_primary_source,
                label=[
                    Text(
                        value=identifier_in_primary_source, language=TextLanguage("en")
                    )
                ],
            )


@watch
def transform_model_nokeda_variable_to_mex_variable_group(
    extracted_cc1_data_model_nokeda: Iterable[Cc1DataModelNoKeda],
    extracted_primary_source: ExtractedPrimarySource,
    mex_resource_nokeda: ExtractedResource,
) -> Generator[ExtractedVariableGroup, None, None]:
    """Transform model nokeda variables to ExtractedVariableGroups.

    Args:
        extracted_cc1_data_model_nokeda: Cc1DataModelNoKeda variables
        extracted_primary_source: Extracted primary source
        mex_resource_nokeda: ExtractedResource

    Returns:
        Generator for ExtractedVariableGroup
    """
    contained_by = mex_resource_nokeda.stableTargetId
    used_identifier_in_primary_source = set()
    for variable in extracted_cc1_data_model_nokeda:
        identifier_in_primary_source = variable.domain_en
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            yield ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                identifierInPrimarySource=identifier_in_primary_source,
                label=[
                    Text(value=variable.domain, language=TextLanguage("de")),
                    Text(value=variable.domain_en, language=TextLanguage("en")),
                ],
            )


@watch
def transform_feat_variable_to_mex_variable_group(
    extracted_cc2_feat_projection: Iterable[Cc2FeatProjection],
    extracted_primary_source: ExtractedPrimarySource,
    mex_resource_feat: ExtractedResource,
) -> Generator[ExtractedVariableGroup, None, None]:
    """Transform feat projection variables to ExtractedVariableGroups.

    Args:
        extracted_cc2_feat_projection: Cc2FeatProjection variables
        extracted_primary_source: Extracted primary source
        mex_resource_feat: ExtractedResource

    Returns:
        Generator for ExtractedVariableGroup
    """
    contained_by = mex_resource_feat.stableTargetId
    used_identifier_in_primary_source = set()
    for variable in extracted_cc2_feat_projection:
        identifier_in_primary_source = " ".join(
            [variable.feature_domain, variable.feature_subdomain]
        )
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            yield ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                identifierInPrimarySource=identifier_in_primary_source,
                label=identifier_in_primary_source,
            )


@watch
def transform_nokeda_model_variable_to_mex_variable(
    extracted_cc1_data_model_nokeda: Iterable[Cc1DataModelNoKeda],
    extracted_cc1_data_valuesets: Iterable[Cc1DataValuesets],
    mex_variable_groups_model_nokeda: Iterable[ExtractedVariableGroup],
    mex_resource_nokeda: ExtractedResource,
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform nokeda model variable to ExtractedVariables.

    Args:
        extracted_cc1_data_model_nokeda: Cc1DataModelNoKeda variables
        extracted_cc1_data_valuesets: Cc1DataValuesets variables
        mex_variable_groups_model_nokeda: variable group nokeda
        mex_resource_nokeda: mex resource nokeda
        extracted_primary_source: Extracted primary source

    Returns:
        Generator for ExtractedVariable
    """
    used_in = mex_resource_nokeda.stableTargetId
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_model_nokeda
        for label in m.label
        if label.language == TextLanguage.DE
    }
    value_sets = list(extracted_cc1_data_valuesets)
    for variable in extracted_cc1_data_model_nokeda:
        value_set = [
            v.category_label_de
            for v in value_sets
            if v.sheet_name == variable.variable_name
        ]
        yield ExtractedVariable(
            dataType=VARIABLE_DATA_TYPE_MAP[variable.type_json],
            belongsTo=stable_target_id_by_label_values[variable.domain],
            description=[
                Text(value=variable.element_description, language=TextLanguage.DE),
                Text(value=variable.element_description_en, language=TextLanguage.EN),
            ],
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=variable.variable_name,
            label=[
                Text(value=variable.element_label, language=TextLanguage.DE),
                Text(value=variable.element_label_en, language=TextLanguage.EN),
            ],
            usedIn=used_in,
            valueSet=value_set,
        )


@watch
def transform_nokeda_aux_variable_to_mex_variable(
    extracted_cc2_aux_model: Iterable[Cc2AuxModel],
    extracted_cc2_aux_mapping: Iterable[Cc2AuxMapping],
    extracted_cc2_aux_valuesets: Iterable[Cc2AuxValuesets],
    mex_variable_groups_nokeda_aux: Iterable[ExtractedVariableGroup],
    mex_resource_nokeda: ExtractedResource,
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform nokeda aux variable to ExtractedVariables.

    Args:
        extracted_cc2_aux_model: Cc2AuxModel variables
        extracted_cc2_aux_mapping: Cc2AuxMapping variables
        extracted_cc2_aux_valuesets: Cc2AuxValuesets variables
        mex_variable_groups_nokeda_aux: variable group nokeda aux
        mex_resource_nokeda: extracted resource
        extracted_primary_source: Extracted primary source

    Returns:
        Generator for ExtractedVariable
    """
    used_in = mex_resource_nokeda.stableTargetId
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_nokeda_aux
        for label in list(m.label)
        if label.language == TextLanguage.EN
    }
    mappings = list(extracted_cc2_aux_mapping)
    value_sets = list(extracted_cc2_aux_valuesets)
    for variable in extracted_cc2_aux_model:
        value_set = []
        for m in mappings:
            if m.sheet_name == variable.depends_on_nokeda_variable:
                value_set.extend(m.variable_name_column)
        if variable.variable_name == "aux_cedis_group":
            for row in value_sets:
                value_set.append(row.label_de)
                value_set.append(row.label_en)
        yield ExtractedVariable(
            belongsTo=stable_target_id_by_label_values[variable.domain],
            description=Text(value=variable.element_description),
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=variable.variable_name,
            label=[Text(value=variable.variable_name)],
            usedIn=used_in,
            valueSet=value_set,
        )


@watch
def transform_feat_projection_variable_to_mex_variable(
    extracted_cc2_feat_projection: Iterable[Cc2FeatProjection],
    mex_variable_groups_feat: Iterable[ExtractedVariableGroup],
    mex_resource_feat: ExtractedResource,
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform feat projection variable to ExtractedVariables.

    Args:
        extracted_cc2_feat_projection: Cc2FeatProjection variables
        mex_variable_groups_feat: variable group feat
        mex_resource_feat: mex resource feat
        extracted_primary_source: Extracted report server primary source

    Returns:
        Generator for ExtractedVariable
    """
    used_in = mex_resource_feat.stableTargetId
    stable_target_id_by_label_values = {
        m.label[0].value: m.stableTargetId for m in mex_variable_groups_feat
    }
    for variable in extracted_cc2_feat_projection:
        yield ExtractedVariable(
            belongsTo=stable_target_id_by_label_values[
                " ".join([variable.feature_domain, variable.feature_subdomain]) or ""
            ],
            description=Text(value=variable.feature_description),
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=" ".join(
                [
                    variable.feature_domain,
                    variable.feature_subdomain,
                    variable.feature_abbr,
                ]
            ),
            label=[
                Text(value=variable.feature_name_de, language=TextLanguage.DE),
                Text(value=variable.feature_name_en, language=TextLanguage.EN),
            ],
            usedIn=used_in,
        )


def transform_sumo_access_platform_to_mex_access_platform(
    sumo_access_platform: dict[str, Any],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    person_stable_target_ids_by_query_string: dict[str, MergedPersonIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
) -> ExtractedAccessPlatform:
    """Transform sumo access platform info to ExtractedAccessPlatform.

    Args:
        sumo_access_platform: sumo_access_platform default values
        unit_merged_ids_by_synonym: Mapping from synonyms to merged IDs of units
        person_stable_target_ids_by_query_string: Mapping from contact person query to
                                                  person stable target ID
        extracted_primary_source: Extracted primary source for sumo

    Returns:
        ExtractedAccessPlatform
    """
    unit_in_charge = [
        unit_merged_ids_by_synonym[unit]
        for unit in sumo_access_platform["unitInCharge"][0]["mappingRules"][0][
            "forValues"
        ]
        if unit
    ]
    if not unit_in_charge[0]:
        unit_in_charge = []
    contact = [
        person_stable_target_ids_by_query_string[contact]
        for contact in sumo_access_platform["contact"][0]["mappingRules"][0][
            "forValues"
        ]
        if contact
    ]
    return ExtractedAccessPlatform(
        identifierInPrimarySource=sumo_access_platform["identifierInPrimarySource"][0][
            "mappingRules"
        ][0]["setValues"],
        hadPrimarySource=extracted_primary_source.stableTargetId,
        title=sumo_access_platform["title"][0]["mappingRules"][0]["setValues"],
        technicalAccessibility=sumo_access_platform["technicalAccessibility"][0][
            "mappingRules"
        ][0]["setValues"],
        unitInCharge=unit_in_charge,
        contact=contact,
    )


def transform_sumo_activity_to_extracted_activity(
    sumo_activity: dict[str, Any],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    contact_merged_ids_by_emails: dict[Email, MergedContactPointIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
) -> ExtractedActivity:
    """Transform sumo activity to ExtractedActivity.

    Args:
        sumo_activity: sumo_activity default values
        unit_merged_ids_by_synonym: Mapping from synonyms to merged IDs of units
        contact_merged_ids_by_emails: Mapping from contact person query to
                                      person stable target ID
        extracted_primary_source: Extracted primary source

    Returns:
        ExtractedActivity
    """
    abstract = sumo_activity["abstract"][0]["mappingRules"][0]["setValues"]
    contact = [
        contact_merged_ids_by_emails[contact]
        for contact in sumo_activity["contact"][0]["mappingRules"][0]["forValues"]
    ]
    documentation = sumo_activity["documentation"][0]["mappingRules"][0]["setValues"]
    involved_unit = [
        unit_merged_ids_by_synonym[unit]
        for unit in sumo_activity["involvedUnit"][0]["mappingRules"][0]["forValues"]
    ]
    publication = sumo_activity["publication"][0]["mappingRules"][0]["setValues"]
    responsible_unit = [
        unit_merged_ids_by_synonym[unit]
        for unit in sumo_activity["responsibleUnit"][0]["mappingRules"][0]["forValues"]
    ]
    short_name = sumo_activity["shortName"][0]["mappingRules"][0]["setValues"]
    title = sumo_activity["title"][0]["mappingRules"][0]["setValues"]
    theme = [
        theme["setValues"][0] for theme in sumo_activity["theme"][0]["mappingRules"]
    ]
    website = sumo_activity["website"][0]["mappingRules"][0]["setValues"]
    return ExtractedActivity(
        abstract=abstract,
        activityType=sumo_activity["activityType"][0]["mappingRules"][0]["setValues"],
        contact=contact,
        documentation=documentation,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=sumo_activity["identifierInPrimarySource"][0][
            "mappingRules"
        ][0]["setValues"],
        involvedUnit=involved_unit,
        publication=publication,
        responsibleUnit=responsible_unit,
        shortName=short_name,
        start=sumo_activity["start"][0]["mappingRules"][0]["setValues"],
        succeeds=[],
        theme=theme,
        title=title,
        website=website,
    )
