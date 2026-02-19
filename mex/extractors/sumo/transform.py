from typing import TYPE_CHECKING

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    Link,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.logging import watch_progress
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
    from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
    from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
    from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
    from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
    from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection


def get_contact_merged_ids_by_emails(
    mex_actor_resources: Iterable[ExtractedContactPoint],
) -> dict[str, MergedContactPointIdentifier]:
    """Get merged id by emails lookup.

    Args:
        mex_actor_resources: Iterable of extracted contact points

    Returns:
        dict of contact merged ids by email
    """
    return {
        email.lower(): MergedContactPointIdentifier(actor.stableTargetId)
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
    sumo_resource_feat: ResourceMapping,
    contact_merged_ids_by_emails: dict[str, MergedContactPointIdentifier],
    mex_resource_nokeda: ExtractedResource,
    transformed_activity: ExtractedActivity,
    sumo_access_platform: ExtractedAccessPlatform,
) -> ExtractedResource:
    """Transform sumo resource_feat to ExtractedResource.

    Args:
        sumo_resource_feat: sumo_resource_feat mapping model
        contact_merged_ids_by_emails: Mapping from emails to merged IDs of contact
                                      points
        mex_resource_nokeda: ExtractedResource for nokeda
        transformed_activity: ExtractedActivity for sumo
        sumo_access_platform: transformed sumo ExtractedAccessPlatform

    Returns:
        ExtractedResource
    """
    keyword = [k.setValues[0] for k in sumo_resource_feat.keyword[0].mappingRules]  # type: ignore[index]
    if (
        contributing_unit_synonym := sumo_resource_feat.contributingUnit[0]
        .mappingRules[0]
        .forValues
    ):
        contributing_unit = get_unit_merged_id_by_synonym(contributing_unit_synonym[0])
    if (
        unit_in_charge_synonym := sumo_resource_feat.unitInCharge[0]
        .mappingRules[0]
        .forValues
    ):
        unit_in_charge = get_unit_merged_id_by_synonym(unit_in_charge_synonym[0])
    return ExtractedResource(
        accessPlatform=[sumo_access_platform.stableTargetId],
        accessRestriction=sumo_resource_feat.accessRestriction[0]
        .mappingRules[0]
        .setValues,
        accrualPeriodicity=sumo_resource_feat.accrualPeriodicity[0]
        .mappingRules[0]
        .setValues,
        contact=[
            contact_merged_ids_by_emails[
                sumo_resource_feat.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
        ],
        contributingUnit=contributing_unit,
        hasPersonalData=sumo_resource_feat.hasPersonalData[0].mappingRules[0].setValues,
        hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
        identifierInPrimarySource=sumo_resource_feat.title[0]
        .mappingRules[0]
        .setValues[0]  # type: ignore[index]
        .value,
        isPartOf=mex_resource_nokeda.stableTargetId,
        keyword=keyword,
        meshId=sumo_resource_feat.meshId[0].mappingRules[0].setValues[0],  # type: ignore[index]
        resourceCreationMethod=sumo_resource_feat.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues[0],  # type: ignore[index]
        resourceTypeGeneral=sumo_resource_feat.resourceTypeGeneral[0]
        .mappingRules[0]
        .setValues,
        resourceTypeSpecific=sumo_resource_feat.resourceTypeSpecific[0]
        .mappingRules[0]
        .setValues,
        spatial=sumo_resource_feat.spatial[0].mappingRules[0].setValues[0],  # type: ignore[index]
        theme=sumo_resource_feat.theme[0].mappingRules[0].setValues,
        title=sumo_resource_feat.title[0].mappingRules[0].setValues[0],  # type: ignore[index]
        unitInCharge=unit_in_charge,
        wasGeneratedBy=transformed_activity.stableTargetId,
    )


def transform_resource_nokeda_to_mex_resource(
    sumo_resource_nokeda: ResourceMapping,
    contact_merged_ids_by_emails: dict[str, MergedContactPointIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    transformed_activity: ExtractedActivity,
    sumo_access_platform: ExtractedAccessPlatform,
) -> ExtractedResource:
    """Transform ResourceNokeda to ExtractedResource.

    Args:
        sumo_resource_nokeda: nokeda resource mapping model with defaults
        contact_merged_ids_by_emails: Mapping from emails to merged IDs of contact
                                      points
        extracted_organization_rki: ExtractedOrganization
        transformed_activity: ExtractedActivity for sumo
        sumo_access_platform: transformed sumo ExtractedAccessPlatform


    Returns:
        ExtractedResource
    """
    if (
        contributing_unit_synonym := sumo_resource_nokeda.contributingUnit[0]
        .mappingRules[0]
        .forValues
    ):
        contributing_unit = get_unit_merged_id_by_synonym(contributing_unit_synonym[0])
    keyword = [k.setValues[0] for k in sumo_resource_nokeda.keyword[0].mappingRules]  # type: ignore[index]
    if (
        unit_in_charge_synonym := sumo_resource_nokeda.unitInCharge[0]
        .mappingRules[0]
        .forValues
    ):
        unit_in_charge = get_unit_merged_id_by_synonym(unit_in_charge_synonym[0])
    return ExtractedResource(
        accessPlatform=[sumo_access_platform.stableTargetId],
        accessRestriction=sumo_resource_nokeda.accessRestriction[0]
        .mappingRules[0]
        .setValues,
        accrualPeriodicity=sumo_resource_nokeda.accrualPeriodicity[0]
        .mappingRules[0]
        .setValues,
        contact=[
            contact_merged_ids_by_emails[
                sumo_resource_nokeda.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
        ],
        contributingUnit=contributing_unit,
        description=[
            Text.model_validate(
                sumo_resource_nokeda.description[0].mappingRules[0].setValues[0]  # type: ignore[index]
            )
        ],
        documentation=[
            Link.model_validate(
                sumo_resource_nokeda.documentation[0].mappingRules[0].setValues[0]  # type: ignore[index]
            )
        ],
        externalPartner=[
            create_new_organization_with_official_name(
                sumo_resource_nokeda.externalPartner[0].mappingRules[0].forValues[0],  # type: ignore[index]
            )
        ],
        hasPersonalData=sumo_resource_nokeda.hasPersonalData[0]
        .mappingRules[0]
        .setValues,
        hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
        identifierInPrimarySource=sumo_resource_nokeda.title[0]
        .mappingRules[0]
        .setValues[0]  # type: ignore[index]
        .value,
        keyword=keyword,
        meshId=sumo_resource_nokeda.meshId[0].mappingRules[0].setValues,
        publication=[],
        publisher=extracted_organization_rki.stableTargetId,
        resourceCreationMethod=sumo_resource_nokeda.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues[0],  # type: ignore[index]
        resourceTypeGeneral=sumo_resource_nokeda.resourceTypeGeneral[0]
        .mappingRules[0]
        .setValues[0],  # type: ignore[index]
        resourceTypeSpecific=sumo_resource_nokeda.resourceTypeSpecific[0]
        .mappingRules[0]
        .setValues,
        rights=[
            Text.model_validate(
                sumo_resource_nokeda.rights[0].mappingRules[0].setValues[0]  # type: ignore[index]
            )
        ],
        spatial=sumo_resource_nokeda.spatial[0].mappingRules[0].setValues[0],  # type: ignore[index]
        stateOfDataProcessing=sumo_resource_nokeda.stateOfDataProcessing[0]
        .mappingRules[0]
        .setValues[0],  # type: ignore[index]
        theme=sumo_resource_nokeda.theme[0].mappingRules[0].setValues,
        title=sumo_resource_nokeda.title[0].mappingRules[0].setValues[0],  # type: ignore[index]
        unitInCharge=unit_in_charge,
        wasGeneratedBy=transformed_activity.stableTargetId,
    )


def transform_nokeda_aux_variable_to_mex_variable_group(
    extracted_cc2_aux_model: Iterable[Cc2AuxModel],
    mex_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform nokeda aux variables to ExtractedVariableGroups.

    Args:
        extracted_cc2_aux_model: Cc2AuxModel variables
        mex_resource_nokeda: ExtractedResource for nokeda

    Returns:
        List of ExtractedVariableGroup
    """
    contained_by = mex_resource_nokeda.stableTargetId
    used_identifier_in_primary_source = set()
    variable_groups = []
    for variable in watch_progress(
        extracted_cc2_aux_model, "transform_nokeda_aux_variable_to_mex_variable_group"
    ):
        identifier_in_primary_source = variable.domain
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            variable_groups.append(
                ExtractedVariableGroup(
                    containedBy=contained_by,
                    hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
                    identifierInPrimarySource=identifier_in_primary_source,
                    label=[
                        Text(
                            value=identifier_in_primary_source,
                            language=TextLanguage("en"),
                        )
                    ],
                )
            )
    return variable_groups


def transform_model_nokeda_variable_to_mex_variable_group(
    extracted_cc1_data_model_nokeda: Iterable[Cc1DataModelNoKeda],
    mex_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform model nokeda variables to ExtractedVariableGroups.

    Args:
        extracted_cc1_data_model_nokeda: Cc1DataModelNoKeda variables
        mex_resource_nokeda: ExtractedResource

    Returns:
        List of ExtractedVariableGroup
    """
    contained_by = mex_resource_nokeda.stableTargetId
    used_identifier_in_primary_source = set()
    variable_groups = []
    for variable in watch_progress(
        extracted_cc1_data_model_nokeda,
        "transform_model_nokeda_variable_to_mex_variable_group",
    ):
        identifier_in_primary_source = variable.domain_en
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            variable_groups.append(
                ExtractedVariableGroup(
                    containedBy=contained_by,
                    hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
                    identifierInPrimarySource=identifier_in_primary_source,
                    label=[
                        Text(value=variable.domain, language=TextLanguage("de")),
                        Text(value=variable.domain_en, language=TextLanguage("en")),
                    ],
                )
            )
    return variable_groups


def transform_feat_variable_to_mex_variable_group(
    sumo_extracted_cc2_feat_projection: Iterable[Cc2FeatProjection],
    mex_resource_feat: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform feat projection variables to ExtractedVariableGroups.

    Args:
        sumo_extracted_cc2_feat_projection: Cc2FeatProjection variables
        mex_resource_feat: ExtractedResource

    Returns:
        List of ExtractedVariableGroup
    """
    contained_by = mex_resource_feat.stableTargetId
    used_identifier_in_primary_source = set()
    variable_groups = []
    for variable in watch_progress(
        sumo_extracted_cc2_feat_projection,
        "transform_feat_variable_to_mex_variable_group",
    ):
        identifier_in_primary_source = (
            f"{variable.feature_domain} {variable.feature_subdomain}"
        )
        if identifier_in_primary_source not in used_identifier_in_primary_source:
            used_identifier_in_primary_source.add(identifier_in_primary_source)
            variable_groups.append(
                ExtractedVariableGroup(
                    containedBy=contained_by,
                    hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
                    identifierInPrimarySource=identifier_in_primary_source,
                    label=identifier_in_primary_source,
                )
            )
    return variable_groups


def transform_nokeda_model_variable_to_mex_variable(
    extracted_cc1_data_model_nokeda: Iterable[Cc1DataModelNoKeda],
    extracted_cc1_data_valuesets: Collection[Cc1DataValuesets],
    mex_variable_groups_model_nokeda: Iterable[ExtractedVariableGroup],
    mex_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform nokeda model variable to ExtractedVariables.

    Args:
        extracted_cc1_data_model_nokeda: Cc1DataModelNoKeda variables
        extracted_cc1_data_valuesets: Cc1DataValuesets variables
        mex_variable_groups_model_nokeda: variable group nokeda
        mex_resource_nokeda: mex resource nokeda

    Returns:
        List of ExtractedVariable
    """
    used_in = mex_resource_nokeda.stableTargetId
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_model_nokeda
        for label in m.label
        if label.language == TextLanguage.DE
    }
    variables = []
    for variable in watch_progress(
        extracted_cc1_data_model_nokeda,
        "transform_nokeda_model_variable_to_mex_variable",
    ):
        value_set = [
            f"{v.category_label_de},{v.category_label_en or ''}"
            for v in extracted_cc1_data_valuesets
            if v.sheet_name == variable.variable_name
        ]
        variables.append(
            ExtractedVariable(
                dataType=variable.type_json,
                belongsTo=stable_target_id_by_label_values[variable.domain],
                description=[
                    Text(value=variable.element_description, language=TextLanguage.DE),
                    Text(
                        value=variable.element_description_en, language=TextLanguage.EN
                    ),
                ],
                hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
                identifierInPrimarySource=variable.variable_name,
                label=[
                    Text(value=variable.element_label, language=TextLanguage.DE),
                    Text(value=variable.element_label_en, language=TextLanguage.EN),
                ],
                usedIn=used_in,
                valueSet=value_set,
            )
        )
    return variables


def transform_nokeda_aux_variable_to_mex_variable(
    extracted_cc2_aux_model: Iterable[Cc2AuxModel],
    extracted_cc2_aux_mapping: Iterable[Cc2AuxMapping],
    extracted_cc2_aux_valuesets: Collection[Cc2AuxValuesets],
    mex_variable_groups_nokeda_aux: Iterable[ExtractedVariableGroup],
    mex_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform nokeda aux variable to ExtractedVariables.

    Args:
        extracted_cc2_aux_model: Cc2AuxModel variables
        extracted_cc2_aux_mapping: Cc2AuxMapping variables
        extracted_cc2_aux_valuesets: Cc2AuxValuesets variables
        mex_variable_groups_nokeda_aux: variable group nokeda aux
        mex_resource_nokeda: extracted resource

    Returns:
        List of ExtractedVariable
    """
    used_in = mex_resource_nokeda.stableTargetId
    stable_target_id_by_label_values = {
        label.value: m.stableTargetId
        for m in mex_variable_groups_nokeda_aux
        for label in m.label
        if label.language == TextLanguage.EN
    }
    value_set_by_sheet_and_variable_name = {
        f"{column.sheet_name}_{column.column_name}": column.variable_name_column
        for column in extracted_cc2_aux_mapping
    }

    variables = []
    for variable in watch_progress(
        extracted_cc2_aux_model, "transform_nokeda_aux_variable_to_mex_variable"
    ):
        value_set = dict.fromkeys(
            value_set_by_sheet_and_variable_name[
                f"{variable.depends_on_nokeda_variable}_{variable.variable_name}"
            ]
        )
        if variable.variable_name == "aux_cedis_group":
            for row in extracted_cc2_aux_valuesets:
                value_set[row.label_de] = None
                value_set[row.label_en] = None
        variables.append(
            ExtractedVariable(
                belongsTo=stable_target_id_by_label_values[variable.domain],
                description=Text(value=variable.element_description),
                hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
                identifierInPrimarySource=variable.variable_name,
                label=[Text(value=variable.variable_name)],
                usedIn=used_in,
                valueSet=list(value_set),
            )
        )
    return variables


def transform_feat_projection_variable_to_mex_variable(
    sumo_extracted_cc2_feat_projection: Iterable[Cc2FeatProjection],
    mex_variable_groups_feat: Iterable[ExtractedVariableGroup],
    mex_resource_feat: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform feat projection variable to ExtractedVariables.

    Args:
        sumo_extracted_cc2_feat_projection: Cc2FeatProjection variables
        mex_variable_groups_feat: variable group feat
        mex_resource_feat: mex resource feat

    Returns:
        List of ExtractedVariable
    """
    used_in = mex_resource_feat.stableTargetId
    stable_target_id_by_label_values = {
        m.label[0].value: m.stableTargetId for m in mex_variable_groups_feat
    }
    return [
        ExtractedVariable(
            belongsTo=stable_target_id_by_label_values[
                f"{variable.feature_domain} {variable.feature_subdomain}" or ""
            ],
            description=Text(value=variable.feature_description),
            hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
            identifierInPrimarySource=(
                f"{variable.feature_domain} {variable.feature_subdomain} "
                f"{variable.feature_abbr}"
            ),
            label=[
                Text(value=variable.feature_name_de, language=TextLanguage.DE),
                Text(value=variable.feature_name_en, language=TextLanguage.EN),
            ],
            usedIn=used_in,
        )
        for variable in watch_progress(
            sumo_extracted_cc2_feat_projection,
            "transform_feat_projection_variable_to_mex_variable",
        )
    ]


def transform_sumo_access_platform_to_mex_access_platform(
    sumo_access_platform: AccessPlatformMapping,
    person_stable_target_ids_by_query_string: dict[str, MergedPersonIdentifier],
) -> ExtractedAccessPlatform:
    """Transform sumo access platform info to ExtractedAccessPlatform.

    Args:
        sumo_access_platform: sumo_access_platform mapping model with default values
        person_stable_target_ids_by_query_string: Mapping from contact person query to
                                                  person stable target ID

    Returns:
        ExtractedAccessPlatform
    """
    unit_in_charge = []
    for unit in sumo_access_platform.unitInCharge[0].mappingRules[0].forValues or ():
        if unit_merged_id := get_unit_merged_id_by_synonym(unit):
            unit_in_charge.extend(unit_merged_id)
    contact = [
        person_stable_target_ids_by_query_string[contact]
        for contact in (sumo_access_platform.contact[0].mappingRules[0].forValues or ())
        if contact
    ]
    return ExtractedAccessPlatform(
        identifierInPrimarySource=sumo_access_platform.identifierInPrimarySource[0]
        .mappingRules[0]
        .setValues,
        hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
        title=sumo_access_platform.title[0].mappingRules[0].setValues,
        technicalAccessibility=sumo_access_platform.technicalAccessibility[0]
        .mappingRules[0]
        .setValues,
        unitInCharge=unit_in_charge,
        contact=contact,
    )


def transform_sumo_activity_to_extracted_activity(
    sumo_activity: ActivityMapping,
    contact_merged_ids_by_emails: dict[str, MergedContactPointIdentifier],
) -> ExtractedActivity:
    """Transform sumo activity to ExtractedActivity.

    Args:
        sumo_activity: sumo_activity mapping model with default values
        contact_merged_ids_by_emails: Mapping from contact person query to
                                      person stable target ID

    Returns:
        ExtractedActivity
    """
    abstract = sumo_activity.abstract[0].mappingRules[0].setValues
    contact = [
        contact_merged_ids_by_emails[contact]
        for contact in (sumo_activity.contact[0].mappingRules[0].forValues or ())
    ]
    documentation = sumo_activity.documentation[0].mappingRules[0].setValues
    involved_unit: list[MergedOrganizationalUnitIdentifier] = []
    if unit_synonyms := sumo_activity.involvedUnit[0].mappingRules[0].forValues:
        for unit in unit_synonyms:
            if unit_merged_id := get_unit_merged_id_by_synonym(unit):
                involved_unit.extend(unit_merged_id)
    responsible_unit: list[MergedOrganizationalUnitIdentifier] = []
    if unit_synonyms := sumo_activity.responsibleUnit[0].mappingRules[0].forValues:
        for unit in unit_synonyms:
            if unit_merged_id := get_unit_merged_id_by_synonym(unit):
                responsible_unit.extend(unit_merged_id)
    short_name = sumo_activity.shortName[0].mappingRules[0].setValues
    title = sumo_activity.title[0].mappingRules[0].setValues
    theme = sumo_activity.theme[0].mappingRules[0].setValues
    website = sumo_activity.website[0].mappingRules[0].setValues
    external_associate = sumo_activity.externalAssociate[0].mappingRules[0].forValues[0]  # type: ignore[index]
    start = sumo_activity.start[0].mappingRules[0].setValues
    activity_type = sumo_activity.activityType[0].mappingRules[0].setValues
    identifier_in_primary_source = (
        sumo_activity.identifierInPrimarySource[0].mappingRules[0].setValues
    )

    return ExtractedActivity(
        abstract=abstract,
        activityType=activity_type,
        contact=contact,
        documentation=documentation,
        hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
        identifierInPrimarySource=identifier_in_primary_source,
        involvedUnit=involved_unit,
        publication=[],  # TODO(KA): add bibliographic resource item
        responsibleUnit=responsible_unit,
        shortName=short_name,
        start=start,
        theme=theme,
        title=title,
        website=website,
        externalAssociate=[
            create_new_organization_with_official_name(external_associate)
        ],
    )


def create_new_organization_with_official_name(
    name: str,
) -> MergedOrganizationIdentifier:
    """Create a new extracted organization with provided name.

    Args:
        name: name of the organization, will be used as official name

    Returns:
        Merged identifier of the organization
    """
    extracted_organization = ExtractedOrganization(
        officialName=[Text(value=name)],
        identifierInPrimarySource=name,
        hadPrimarySource=get_extracted_primary_source_id_by_name("nokeda"),
    )
    load([extracted_organization])
    return MergedOrganizationIdentifier(extracted_organization.stableTargetId)
