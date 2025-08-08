import re
from collections.abc import Generator, Iterable
from itertools import groupby, tee
from pathlib import PureWindowsPath
from types import NoneType
from typing import cast, get_args

from pydantic import ValidationError

from mex.common.logging import watch
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    Identifier,
    Link,
    MergedAccessPlatformIdentifier,
    MergedActivityIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TemporalEntity,
    Text,
    TextLanguage,
)
from mex.extractors.sinks import load
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable


def transform_synopse_studies_into_access_platforms(
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
    contact_merged_id_by_query_string: dict[str, MergedContactPointIdentifier],
    access_platform_mapping: AccessPlatformMapping,
) -> ExtractedAccessPlatform:
    """Transform synopse studies into access platforms.

    Args:
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        extracted_primary_source: Extracted report server primary source
        contact_merged_id_by_query_string: contact person lookup by email
        access_platform_mapping: mapping default values for access platform
    Returns:
        extracted access platform
    """
    return ExtractedAccessPlatform(
        alternativeTitle=access_platform_mapping.alternativeTitle[0]
        .mappingRules[0]
        .setValues,
        contact=contact_merged_id_by_query_string[
            access_platform_mapping.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ],
        description=access_platform_mapping.description[0].mappingRules[0].setValues,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=access_platform_mapping.identifierInPrimarySource[0]
        .mappingRules[0]
        .setValues[0],  # type: ignore[index]
        landingPage=access_platform_mapping.landingPage[0].mappingRules[0].setValues,
        technicalAccessibility=access_platform_mapping.technicalAccessibility[0]
        .mappingRules[0]
        .setValues,
        title=access_platform_mapping.title[0].mappingRules[0].setValues,
        unitInCharge=unit_merged_ids_by_synonym[
            access_platform_mapping.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ],
    )


def transform_overviews_to_resource_lookup(
    study_overviews: list[SynopseStudyOverview],
    study_resources: list[ExtractedResource],
) -> dict[str, ExtractedResource]:
    """Transform overviews and resources into a resource ID lookup.

    Args:
        study_overviews: list of Synopse Overviews
        study_resources: list of Study Resources

    Returns:
        Map from synopse variable ID to list of resource stable target IDs
    """
    resource_by_identifier_in_platform = {
        resource.identifierInPrimarySource: resource for resource in study_resources
    }
    resources_by_synopse_id: dict[str, ExtractedResource] = {}
    for study in study_overviews:
        if resource := resource_by_identifier_in_platform.get(
            f"{study.studien_id}-{study.titel_datenset}-{study.ds_typ_id}"
        ):
            resources_by_synopse_id[study.synopse_id] = resource
        else:
            continue
    return resources_by_synopse_id


@watch()
def transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
    variables: Iterable[SynopseVariable],
    belongs_to: ExtractedVariableGroup,
    resources_by_synopse_id: dict[str, ExtractedResource],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform Synopse variables to extracted variables.

    Args:
        variables: Iterable of Synopse Variables
        belongs_to: extracted variable group that the variables belong to
        resources_by_synopse_id: Map from synopse ID to study resource
        extracted_primary_source: Extracted report server primary source

    Returns:
        Generator for ExtractedVariable
    """
    # groupby requires sorted iterable
    variables = sorted(variables, key=lambda x: x.synopse_id)

    for synopse_id, levels_iter in groupby(variables, lambda x: x.synopse_id):
        levels = list(levels_iter)
        variable = levels[0]
        if variable.synopse_id in resources_by_synopse_id:
            used_in = resources_by_synopse_id[variable.synopse_id].stableTargetId
        else:
            continue

        yield ExtractedVariable(
            belongsTo=[belongs_to.stableTargetId] if belongs_to else [],
            codingSystem=variable.val_instrument,
            dataType=variable.datentyp,
            description=(
                Text(value=variable.originalfrage, language=TextLanguage("de"))
                if variable.originalfrage
                else []
            ),
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=synopse_id,
            label=variable.varlabel or variable.varname,
            usedIn=used_in,
            valueSet=sorted({level.text_dt for level in levels if level.text_dt}),
        )


@watch()
def transform_synopse_variables_to_mex_variables(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    variable_groups: Iterable[ExtractedVariableGroup],
    resources_by_synopse_id: dict[str, ExtractedResource],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform Synopse Variable Sets to MEx datums.

    Args:
        synopse_variables_by_thema: mapping from "Thema und Fragebogenausschnitt"
            to the variables having this value
        variable_groups: extracted variable groups
        resources_by_synopse_id: Map from synopse ID to study resource
        extracted_primary_source: Extracted report server primary source

    Returns:
        Generator for ExtractedVariable
    """
    variable_group_by_thema = {
        group.identifierInPrimarySource.split("-")[0]: group
        for group in variable_groups
    }
    for thema, variables in synopse_variables_by_thema.items():
        if thema not in variable_group_by_thema:
            continue
        belongs_to = variable_group_by_thema[thema]
        yield from transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(  # noqa: E501
            variables,
            belongs_to,
            resources_by_synopse_id,
            extracted_primary_source,
        )


def transform_synopse_variables_to_mex_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source: ExtractedPrimarySource,
    resources_by_synopse_id: dict[str, ExtractedResource],
) -> list[ExtractedVariableGroup]:
    """Transform Synopse Variable Sets to MEx Variable Groups.

    Args:
        synopse_variables_by_thema: mapping from "Thema und Fragebogenausschnitt"
            to the variables having this value
        extracted_primary_source: Extracted report server primary source
        resources_by_synopse_id: Map from synopse ID to list of study resources

    Returns:
        list of extracted variable groups
    """
    variable_groups: list[ExtractedVariableGroup] = []
    for thema, variables in synopse_variables_by_thema.items():
        synopse_ids = {v.synopse_id for v in variables}

        contained_by = list(
            {
                resource.stableTargetId
                for synopse_id in synopse_ids
                if (resource := resources_by_synopse_id.get(synopse_id))
            }
        )
        for resource_identifier_in_primary_source in list(
            {
                resource.identifierInPrimarySource
                for synopse_id in synopse_ids
                if (resource := resources_by_synopse_id.get(synopse_id))
            }
        ):
            if len(resource_identifier_in_primary_source) > 0:
                identifier_in_primary_source = (
                    f"{thema}-{resource_identifier_in_primary_source}"
                )
            else:
                continue

            label = Text(
                value=re.sub(r"\s\(\d+\)", "", thema), language=TextLanguage("de")
            )
            if contained_by:
                variable_groups.append(
                    ExtractedVariableGroup(
                        containedBy=contained_by,
                        hadPrimarySource=extracted_primary_source.stableTargetId,
                        identifierInPrimarySource=identifier_in_primary_source,
                        label=label,
                    )
                )
    return variable_groups


def transform_synopse_data_to_mex_resources(  # noqa: C901, PLR0912, PLR0913, PLR0915
    synopse_studies: Iterable[SynopseStudy],
    synopse_projects: Iterable[SynopseProject],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    extracted_activities: Iterable[ExtractedActivity],
    extracted_primary_source: ExtractedPrimarySource,
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization: ExtractedOrganization,
    synopse_resource: ResourceMapping,
    extracted_synopse_access_platform_id: MergedAccessPlatformIdentifier,
    extracted_synopse_contributor_stable_target_ids_by_name: dict[
        str, list[MergedPersonIdentifier]
    ],
) -> list[ExtractedResource]:
    """Transform Synopse Studies to MEx resources.

    Args:
        synopse_studies: Iterable of Synopse Studies
        synopse_projects: Iterable of synopse projects
        synopse_variables_by_study_id: mapping from synopse studie id to the variables
            with this studie id
        extracted_activities: Iterable of extracted activities
        extracted_primary_source: Extracted report server platform
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        extracted_organization: extracted organization
        synopse_resource: resource default values
        contact_merged_id_by_query_string: contact person lookup by email
        extracted_synopse_access_platform_id: synopse access platform id
        extracted_synopse_contributor_stable_target_ids_by_name: person ids by name

    Returns:
        list for extracted resources
    """
    access_restriction_by_zugangsbeschraenkung = {
        for_value: rule.setValues
        for rule in synopse_resource.accessRestriction[0].mappingRules
        if rule.forValues
        for for_value in rule.forValues
    }
    extracted_activities_by_study_ids = {
        a.identifierInPrimarySource: a for a in extracted_activities
    }
    contact_point = unit_merged_ids_by_synonym.get(
        synopse_resource.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
    )
    synopse_projects_by_study_ids = {p.studien_id: p for p in synopse_projects}
    synopse_studies_gens = tee(synopse_studies, 2)
    description_by_study_id: dict[str, str | None] = {}
    identifier_in_primary_source_by_study_id: dict[str, str] = {}
    title_by_study_id: dict[str, Text] = {}
    rights_by_ds_typ_id = {
        rule.forValues[0]: rule.setValues  # type: ignore[index]
        for rule in synopse_resource.rights[0].mappingRules
    }
    extracted_resources: list[ExtractedResource] = []
    for study in synopse_studies_gens[0]:
        if study.studien_id in synopse_projects_by_study_ids:
            project = synopse_projects_by_study_ids[study.studien_id]
        else:
            continue
        access_platform: list[MergedAccessPlatformIdentifier] = []
        if synopse_resource.accessPlatform[0].mappingRules[0].forValues and (
            str(study.ds_typ_id)
            in synopse_resource.accessPlatform[0].mappingRules[0].forValues
        ):
            access_platform.append(extracted_synopse_access_platform_id)
        if (
            zugangsbeschraenkung := study.zugangsbeschraenkung.split(";")[0]
        ) and zugangsbeschraenkung in access_restriction_by_zugangsbeschraenkung:
            access_restriction = access_restriction_by_zugangsbeschraenkung[
                zugangsbeschraenkung
            ]
        else:
            continue
        contact: list[
            MergedOrganizationalUnitIdentifier
            | MergedPersonIdentifier
            | MergedContactPointIdentifier
        ] = []
        if (
            contact_point
            and study.ds_typ_id
            == synopse_resource.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ):
            contact.append(contact_point)
        if project.verantwortliche_oe in unit_merged_ids_by_synonym:
            contact.append(unit_merged_ids_by_synonym[project.verantwortliche_oe])
        else:
            continue
        contributing_unit: list[MergedOrganizationalUnitIdentifier] = []
        if project.verantwortliche_oe in unit_merged_ids_by_synonym:
            contributing_unit = [unit_merged_ids_by_synonym[project.verantwortliche_oe]]
        contributor: list[MergedPersonIdentifier] = []
        if (
            project.beitragende
            in extracted_synopse_contributor_stable_target_ids_by_name
        ):
            contributor = extracted_synopse_contributor_stable_target_ids_by_name[
                project.beitragende
            ]
        description_by_study_id[study.studien_id] = study.beschreibung
        synopse_variables = synopse_variables_by_study_id.get(int(study.studien_id))
        keywords_plain = []
        if study.schlagworte_themen:
            keywords_plain += list(re.split(r"\s*,\s*", study.schlagworte_themen))
        if synopse_variables:
            # add unique values of all textbox 2 entries from variable set for this
            # study, remove suffix, e.g "Schlagwort (12345)" -> "Schlagwort"
            keywords_plain.extend(
                {re.sub(r"\s\(\d+\)", "", var.unterthema) for var in synopse_variables}
            )
        keyword = [
            Text(value=word, language=TextLanguage.DE) for word in keywords_plain
        ]
        identifier_in_primary_source_by_study_id[study.studien_id] = (
            f"{study.studien_id}-{study.titel_datenset}-{study.ds_typ_id}"
        )
        title_by_study_id[study.studien_id] = Text(
            value=study.titel_datenset, language=TextLanguage("de")
        )
        description = (
            [Text(value=study.beschreibung, language=TextLanguage.DE)]
            if study.beschreibung
            else []
        )
        has_legal_basis = (
            [Text(value=study.rechte, language=TextLanguage.DE)] if study.rechte else []
        )
        has_purpose = (
            [Text(value=study.zweck, language=TextLanguage.DE)] if study.zweck else []
        )
        extracted_activity = extracted_activities_by_study_ids.get(study.studien_id)
        modified = None
        for typ in get_args(ExtractedResource.model_fields["modified"].annotation):
            if study.datum_der_letzten_aenderung and typ is not NoneType:
                try:
                    modified = typ(study.datum_der_letzten_aenderung)
                    break
                except ValidationError:
                    continue
        population_coverage = (
            [Text(value=study.bevoelkerungsabdeckung, language=TextLanguage.DE)]
            if study.bevoelkerungsabdeckung
            else []
        )
        provenance = (
            [Text(value=study.herkunft_der_daten, language=TextLanguage.DE)]
            if study.herkunft_der_daten
            else []
        )
        resource_type_specific = (
            [Text(value=project.studienart_studientyp, language=TextLanguage.DE)]
            if project.studienart_studientyp
            else []
        )
        rights = rights_by_ds_typ_id[str(study.ds_typ_id)]
        temporal = None
        if study.feld_start and study.feld_ende:
            temporal = f"{study.feld_start}-{study.feld_ende}"
        theme = (
            synopse_resource.theme[0].mappingRules[0].setValues
            if study.studien_id
            in (synopse_resource.theme[0].mappingRules[0].forValues or ())
            else synopse_resource.theme[0].mappingRules[1].setValues
        )
        if project.verantwortliche_oe in unit_merged_ids_by_synonym:
            unit_in_charge = unit_merged_ids_by_synonym[project.verantwortliche_oe]
        else:
            continue
        extracted_resources.append(
            ExtractedResource(
                accessPlatform=access_platform,
                accessRestriction=access_restriction,
                contact=contact,
                contributingUnit=contributing_unit,
                contributor=contributor,
                description=description,
                hasLegalBasis=has_legal_basis,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                hasPurpose=has_purpose,
                identifierInPrimarySource=identifier_in_primary_source_by_study_id[
                    study.studien_id
                ],
                keyword=keyword,
                language=synopse_resource.language[0].mappingRules[0].setValues,
                maxTypicalAge=study.typisches_alter_max,
                minTypicalAge=study.typisches_alter_min,
                modified=modified,
                populationCoverage=population_coverage,
                provenance=provenance,
                publisher=[extracted_organization.stableTargetId],
                resourceCreationMethod=synopse_resource.resourceCreationMethod[0]
                .mappingRules[0]
                .setValues,
                resourceTypeGeneral=synopse_resource.resourceTypeGeneral[0]
                .mappingRules[0]
                .setValues,
                resourceTypeSpecific=resource_type_specific,
                rights=rights,
                spatial=study.raeumlicher_bezug,
                temporal=temporal,
                theme=theme,
                title=title_by_study_id[study.studien_id],
                unitInCharge=unit_in_charge,
                wasGeneratedBy=(
                    extracted_activity.stableTargetId if extracted_activity else None
                ),
            )
        )
    return extracted_resources


def transform_synopse_projects_to_mex_activities(  # noqa: PLR0913
    synopse_projects: Iterable[SynopseProject],
    extracted_primary_source: ExtractedPrimarySource,
    contributor_merged_ids_by_name: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_activity: ActivityMapping,
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> tuple[list[ExtractedActivity], list[ExtractedActivity]]:
    """Transform synopse projects into MEx activities.

    Args:
        synopse_projects: Iterable of synopse projects
        extracted_primary_source: Extracted report server primary sources
        contact_merged_ids_by_emails: Mapping from LDAP emails to contact IDs
        contributor_merged_ids_by_name: Mapping from person names to contributor IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        synopse_activity: synopse activity default values
        synopse_organization_ids_by_query_string: merged organization ids by org name

    Returns:
        tuple of non-child and child extracted activities
    """
    activity_stable_target_id_by_short_name: dict[str, Identifier] = {}
    anschlussprojekt_by_activity_stable_target_id: dict[Identifier, str] = {}
    activities = []

    # transform without setting succeeds because the activity that is referenced might
    # not yet be transformed
    for project in synopse_projects:
        anschlussprojekt = (
            project.anschlussprojekt.removesuffix("-Basiserhebung")
            if project.anschlussprojekt == "KiGGS-Basiserhebung"
            else project.anschlussprojekt
        )
        activity = transform_synopse_project_to_activity(
            project,
            extracted_primary_source,
            contributor_merged_ids_by_name,
            unit_merged_ids_by_synonym,
            synopse_activity,
            synopse_organization_ids_by_query_string,
        )
        if not activity:
            continue
        if anschlussprojekt:
            anschlussprojekt_by_activity_stable_target_id[activity.stableTargetId] = (
                anschlussprojekt
            )
        activity_stable_target_id_by_short_name[activity.shortName[0].value] = (
            activity.stableTargetId
        )
        activities.append(activity)

    # set succeeds
    non_child_activities: list[ExtractedActivity] = []
    child_activities: list[ExtractedActivity] = []
    for activity in activities:
        if anschlussprojekt := anschlussprojekt_by_activity_stable_target_id.get(
            activity.stableTargetId
        ):
            activity.succeeds = [
                cast(
                    "MergedActivityIdentifier",
                    activity_stable_target_id_by_short_name[anschlussprojekt],
                )
            ]
            child_activities.append(activity)
        else:
            non_child_activities.append(activity)
    return non_child_activities, child_activities


def transform_synopse_project_to_activity(  # noqa: C901, PLR0912, PLR0913
    synopse_project: SynopseProject,
    extracted_primary_source: ExtractedPrimarySource,
    contributor_merged_ids_by_name: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_activity: ActivityMapping,
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> ExtractedActivity | None:
    """Transform a synopse project into a MEx activity.

    Args:
        synopse_project: a synopse project
        extracted_primary_source: Extracted report server primary sources
        contact_merged_ids_by_emails: Mapping from LDAP emails to contact IDs
        contributor_merged_ids_by_name: Mapping from person names to contributor IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        synopse_activity: synopse activity default values
        synopse_organization_ids_by_query_string: merged organization ids by org name

    Returns:
        extracted activity
    """
    if synopse_project.verantwortliche_oe in unit_merged_ids_by_synonym:
        contact = unit_merged_ids_by_synonym.get(synopse_project.verantwortliche_oe)
    else:
        return None
    documentation = None
    if projektdokumentation := synopse_project.projektdokumentation:
        try:
            path_line, *remaining_lines = projektdokumentation.strip().splitlines()
            title_lines = [line for line in remaining_lines if line.startswith("-")]
            documentation = Link(
                url=PureWindowsPath(path_line).as_uri(), title="\n".join(title_lines)
            )
        except ValueError:
            pass  # TODO(HS): handle relative paths
    involved_units = [
        merged_id
        for unit in (synopse_project.interne_partner or "").split(" ,")
        if (merged_id := unit_merged_ids_by_synonym.get(unit.strip()))
    ]

    if synopse_project.verantwortliche_oe and any(
        unit in synopse_project.verantwortliche_oe.split(" ,")
        for unit in unit_merged_ids_by_synonym
    ):
        responsible_unit = [
            unit_merged_ids_by_synonym[synonym]
            for synonym in synopse_project.verantwortliche_oe.split(" ,")
            if synonym in unit_merged_ids_by_synonym
        ]
    else:
        responsible_unit = [unit_merged_ids_by_synonym["FG21"]]
    external_associate = []
    if synopse_project.externe_partner:
        for org in synopse_project.externe_partner.split(", "):
            if org in synopse_organization_ids_by_query_string:
                external_associate.append(synopse_organization_ids_by_query_string[org])
            else:
                extracted_organization = ExtractedOrganization(
                    officialName=org,
                    identifierInPrimarySource=org,
                    hadPrimarySource=extracted_primary_source.stableTargetId,
                )
                load([extracted_organization])
                external_associate.append(
                    MergedOrganizationIdentifier(extracted_organization.stableTargetId)
                )

    if (
        synopse_project.foerderinstitution_oder_auftraggeber
        in synopse_organization_ids_by_query_string
    ):
        funder_or_commissioner = [
            synopse_organization_ids_by_query_string[
                synopse_project.foerderinstitution_oder_auftraggeber
            ]
        ]
    elif synopse_project.foerderinstitution_oder_auftraggeber:
        extracted_organization = ExtractedOrganization(
            officialName=synopse_project.foerderinstitution_oder_auftraggeber,
            identifierInPrimarySource=synopse_project.foerderinstitution_oder_auftraggeber,
            hadPrimarySource=extracted_primary_source.stableTargetId,
        )
        load([extracted_organization])
        funder_or_commissioner = [
            MergedOrganizationIdentifier(extracted_organization.stableTargetId)
        ]
    else:
        funder_or_commissioner = []
    involved_person = []
    if synopse_project.beitragende:
        involved_person = [
            merged_id
            for name in synopse_project.beitragende.split(" ,")
            for merged_id in contributor_merged_ids_by_name[name]
        ] or []
    theme = (
        synopse_activity.theme[0].mappingRules[0].setValues
        if synopse_project.studien_id
        in (synopse_activity.theme[0].mappingRules[0].forValues or ())
        else synopse_activity.theme[0].mappingRules[1].setValues
    )
    return ExtractedActivity(
        abstract=synopse_project.beschreibung_der_studie,
        activityType=synopse_activity.activityType[0].mappingRules[0].setValues,
        contact=contact,
        documentation=documentation,
        end=(
            TemporalEntity(synopse_project.projektende)
            if synopse_project.projektende
            else None
        ),
        externalAssociate=external_associate,
        funderOrCommissioner=funder_or_commissioner,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        identifierInPrimarySource=synopse_project.studien_id,
        involvedPerson=involved_person,
        involvedUnit=involved_units,
        responsibleUnit=responsible_unit,
        shortName=Text(
            value=synopse_project.akronym_des_studientitels, language=TextLanguage.DE
        ),
        start=(
            TemporalEntity(synopse_project.projektbeginn)
            if synopse_project.projektbeginn
            else None
        ),
        theme=theme,
        title=synopse_project.project_studientitel
        or synopse_project.akronym_des_studientitels,
    )
