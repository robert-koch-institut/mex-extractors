import re
from collections.abc import Generator, Iterable, Mapping
from itertools import groupby, tee
from pathlib import PureWindowsPath
from typing import cast

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
    MergedActivityIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
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
    synopse_studies: Iterable[SynopseStudy],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source: ExtractedPrimarySource,
    synopse_access_platform: AccessPlatformMapping,
) -> Generator[ExtractedAccessPlatform, None, None]:
    """Transform synopse studies into access platforms.

    Args:
        synopse_studies: Iterable of Synopse Studies
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        extracted_primary_source: Extracted report server primary source
        synopse_access_platform: access platform mapping model with default values

    Returns:
        extracted access platform
    """
    for plattform_adresse in sorted(
        {
            study.plattform_adresse
            for study in synopse_studies
            if study.plattform_adresse is not None
        }
    ):
        if (
            plattform_adresse
            == synopse_access_platform.landingPage[0].mappingRules[1].forValues[0]  # type: ignore[index]
        ):
            landing_page = (
                synopse_access_platform.landingPage[0].mappingRules[1].setValues[0]  # type: ignore[index]
            )
        else:
            try:
                landing_page = Link(url=PureWindowsPath(plattform_adresse).as_uri())
            except ValueError:
                landing_page = Link(url=plattform_adresse)
        if "S:" in plattform_adresse:
            contact = unit_merged_ids_by_synonym[
                synopse_access_platform.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
            technical_accessibility = (
                synopse_access_platform.technicalAccessibility[0]
                .mappingRules[0]
                .setValues
            )
            title = Text(value=plattform_adresse)
            unit_in_charge = unit_merged_ids_by_synonym[
                synopse_access_platform.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
            ]
        elif "https://" in plattform_adresse:
            contact = unit_merged_ids_by_synonym[
                synopse_access_platform.contact[0].mappingRules[1].forValues[0]  # type: ignore[index]
            ]
            technical_accessibility = (
                synopse_access_platform.technicalAccessibility[0]
                .mappingRules[1]
                .setValues
            )
            title = synopse_access_platform.title[0].mappingRules[1].setValues[0]  # type: ignore[index]
            unit_in_charge = unit_merged_ids_by_synonym[
                synopse_access_platform.unitInCharge[0].mappingRules[1].forValues[0]  # type: ignore[index]
            ]
        yield ExtractedAccessPlatform(
            contact=contact,
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=plattform_adresse,
            landingPage=landing_page,
            technicalAccessibility=technical_accessibility,
            title=title,
            unitInCharge=unit_in_charge,
        )


def transform_overviews_to_resource_lookup(
    study_overviews: Iterable[SynopseStudyOverview],
    study_resources: Iterable[ExtractedResource],
) -> dict[str, list[MergedResourceIdentifier]]:
    """Transform overviews and resources into a resource ID lookup.

    Args:
        study_overviews: Iterable of Synopse Overviews
        study_resources: Iterable of Study Resources

    Returns:
        Map from synopse variable ID to list of resource stable target IDs
    """
    resource_id_by_identifier_in_platform = {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in study_resources
    }
    resource_ids_by_synopse_id: dict[str, list[MergedResourceIdentifier]] = {}
    for study in study_overviews:
        if resource_id := resource_id_by_identifier_in_platform.get(
            f"{study.studien_id}-{study.titel_datenset}-{study.ds_typ_id}"
        ):
            resource_ids = resource_ids_by_synopse_id.setdefault(study.synopse_id, [])
            resource_ids.append(MergedResourceIdentifier(resource_id))
        else:
            continue
    return resource_ids_by_synopse_id


@watch()
def transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(
    variables: Iterable[SynopseVariable],
    belongs_to: ExtractedVariableGroup,
    resource_ids_by_synopse_id: dict[str, list[MergedResourceIdentifier]],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform Synopse Variables to MEx datums.

    Args:
        variables: Iterable of Synopse Variables
        belongs_to: extracted variable group that the variables belong to
        resource_ids_by_synopse_id: Map from synopse ID to list of study resources
            stable target id
        extracted_primary_source: Extracted report server primary source

    Returns:
        Generator for ExtractedVariable
    """
    # groupby requires sorted iterable
    variables = sorted(variables, key=lambda x: x.synopse_id)

    for synopse_id, levels_iter in groupby(variables, lambda x: x.synopse_id):
        levels = list(levels_iter)
        variable = levels[0]
        if variable.synopse_id in resource_ids_by_synopse_id:
            used_in = resource_ids_by_synopse_id[variable.synopse_id]
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
    resource_ids_by_synopse_id: dict[str, list[MergedResourceIdentifier]],
    extracted_primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedVariable, None, None]:
    """Transform Synopse Variable Sets to MEx datums.

    Args:
        synopse_variables_by_thema: mapping from "Thema und Fragebogenausschnitt"
            to the variables having this value
        variable_groups: extracted variable groups
        resource_ids_by_synopse_id: Map from synopse ID to list of study resources
            stable target id
        extracted_primary_source: Extracted report server primary source

    Returns:
        Generator for ExtractedVariable
    """
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group for group in variable_groups
    }
    for thema, variables in synopse_variables_by_thema.items():
        if thema not in variable_group_by_identifier_in_primary_source:
            continue
        belongs_to = variable_group_by_identifier_in_primary_source[thema]
        yield from transform_synopse_variables_belonging_to_same_variable_group_to_mex_variables(  # noqa: E501
            variables,
            belongs_to,
            resource_ids_by_synopse_id,
            extracted_primary_source,
        )


@watch()
def transform_synopse_variables_to_mex_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source: ExtractedPrimarySource,
    resource_ids_by_synopse_id: dict[str, list[MergedResourceIdentifier]],
) -> Generator[ExtractedVariableGroup, None, None]:
    """Transform Synopse Variable Sets to MEx Variable Groups.

    Args:
        synopse_variables_by_thema: mapping from "Thema und Fragebogenausschnitt"
            to the variables having this value
        extracted_primary_source: Extracted report server primary source
        resource_ids_by_synopse_id: Map from synopse ID to list of study resources
            stable target id

    Returns:
        Generator for extracted variable groups
    """
    for thema, variables in synopse_variables_by_thema.items():
        synopse_ids = {v.synopse_id for v in variables}
        contained_by = list(
            {
                resource_id
                for synopse_id in synopse_ids
                if (resource_ids := resource_ids_by_synopse_id.get(synopse_id))
                for resource_id in resource_ids
            }
        )

        label = Text(value=re.sub(r"\s\(\d+\)", "", thema), language=TextLanguage("de"))
        if contained_by:
            yield ExtractedVariableGroup(
                containedBy=contained_by,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                identifierInPrimarySource=thema,
                label=label,
            )


@watch()
def transform_synopse_data_to_mex_resources(  # noqa: PLR0913
    synopse_studies: Iterable[SynopseStudy],
    synopse_projects: Iterable[SynopseProject],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    extracted_activities: Iterable[ExtractedActivity],
    extracted_access_platforms: Iterable[ExtractedAccessPlatform],
    extracted_primary_source: ExtractedPrimarySource,
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization: ExtractedOrganization,
    synopse_resource: ResourceMapping,
    contact_merged_id_by_query_string: dict[str, MergedContactPointIdentifier],
) -> Generator[ExtractedResource, None, None]:
    """Transform Synopse Studies to MEx resources.

    Args:
        synopse_studies: Iterable of Synopse Studies
        synopse_projects: Iterable of synopse projects
        synopse_variables_by_study_id: mapping from synopse studie id to the variables
            with this studie id
        extracted_activities: Iterable of extracted activities
        extracted_access_platforms: Iterable of extracted access platforms
        extracted_primary_source: Extracted report server platform
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        extracted_organization: extracted organization
        synopse_resource: resource default values
        contact_merged_id_by_query_string: contact person lookup by email


    Returns:
        Generator for extracted resources
    """
    extracted_activities_by_study_ids = {
        a.identifierInPrimarySource: a for a in extracted_activities
    }
    contact = contact_merged_id_by_query_string[
        synopse_resource.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
    ]
    access_platform_by_identifier_in_primary_source = {
        p.identifierInPrimarySource: p for p in extracted_access_platforms
    }
    synopse_studien_art_typ_by_study_ids = {
        p.studien_id: p.studienart_studientyp for p in synopse_projects
    }
    synopse_studies_gens = tee(synopse_studies, 2)
    created_by_study_id: dict[str, str | None] = {}
    description_by_study_id: dict[str, str | None] = {}
    documentation_by_study_id: dict[str, Link | None] = {}
    identifier_in_primary_source_by_study_id: dict[str, str] = {}
    title_by_study_id: dict[str, Text] = {}
    rights_by_ds_typ_id = {
        rule.forValues[0]: rule.setValues  # type: ignore[index]
        for rule in synopse_resource.rights[0].mappingRules
    }
    for study in synopse_studies_gens[0]:
        created_by_study_id[study.studien_id] = study.erstellungs_datum
        description_by_study_id[study.studien_id] = study.beschreibung
        synopse_variables = synopse_variables_by_study_id.get(int(study.studien_id))
        if not study.dokumentation:
            documentation = None
        elif re.match(r"^[a-zA-Z]:\\.*$", study.dokumentation):
            documentation = Link(url=PureWindowsPath(study.dokumentation).as_uri())
        elif re.match(r"https?://.*", study.dokumentation):
            documentation = Link(url=study.dokumentation)
        else:
            documentation = None
        documentation_by_study_id[study.studien_id] = documentation
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
        access_platform = (
            access_platform_by_identifier_in_primary_source[
                study.plattform_adresse
            ].stableTargetId
            if study.plattform_adresse
            in access_platform_by_identifier_in_primary_source
            else []
        )
        created = created_by_study_id.get(study.studien_id)
        description = description_by_study_id.get(study.studien_id)
        documentation = documentation_by_study_id.get(study.studien_id)
        extracted_activity = extracted_activities_by_study_ids.get(study.studien_id)
        rights = rights_by_ds_typ_id[str(study.ds_typ_id)]
        theme = (
            synopse_resource.theme[0].mappingRules[0].setValues
            if study.studien_id
            in (synopse_resource.theme[0].mappingRules[0].forValues or ())
            else synopse_resource.theme[0].mappingRules[1].setValues
        )
        unit_in_charge = unit_merged_ids_by_synonym[
            synopse_resource.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ]
        yield ExtractedResource(
            accessPlatform=access_platform,
            accessRestriction=synopse_resource.accessRestriction[0]
            .mappingRules[0]
            .setValues,
            contact=contact,
            contributingUnit=(
                extracted_activity.involvedUnit if extracted_activity else None
            ),
            contributor=(
                extracted_activity.involvedPerson if extracted_activity else None
            ),
            created=created,
            description=description,
            documentation=documentation,
            hasLegalBasis=[study.rechte] if study.rechte else [],
            hasPersonalData=synopse_resource.hasPersonalData[0]
            .mappingRules[0]
            .setValues,
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=identifier_in_primary_source_by_study_id[
                study.studien_id
            ],
            keyword=keyword,
            language=synopse_resource.language[0].mappingRules[0].setValues,
            publisher=[extracted_organization.stableTargetId],
            resourceCreationMethod=synopse_resource.resourceCreationMethod[0]
            .mappingRules[0]
            .setValues,
            resourceTypeGeneral=synopse_resource.resourceTypeGeneral[0]
            .mappingRules[0]
            .setValues,
            resourceTypeSpecific=synopse_studien_art_typ_by_study_ids.get(
                study.studien_id, []
            ),
            rights=rights,
            spatial=synopse_resource.spatial[0].mappingRules[0].setValues,
            temporal=(
                " - ".join(
                    [
                        min(extracted_activity.start).date_time.strftime("%Y"),
                        max(extracted_activity.end).date_time.strftime("%Y"),
                    ]
                )
                if extracted_activity
                and extracted_activity.start
                and extracted_activity.end
                else None
            ),
            theme=theme,
            title=title_by_study_id[study.studien_id],
            unitInCharge=unit_in_charge,
            wasGeneratedBy=(
                extracted_activity.stableTargetId if extracted_activity else None
            ),
        )


def transform_synopse_projects_to_mex_activities(  # noqa: PLR0913
    synopse_projects: Iterable[SynopseProject],
    extracted_primary_source: ExtractedPrimarySource,
    contributor_merged_ids_by_name: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_activity: ActivityMapping,
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    contact_merged_id_by_query_string: Mapping[
        str,
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier,
    ],
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
        contact_merged_id_by_query_string: contact merged identifier lookup by email

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
            contact_merged_id_by_query_string,
        )
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


def transform_synopse_project_to_activity(  # noqa: PLR0913
    synopse_project: SynopseProject,
    extracted_primary_source: ExtractedPrimarySource,
    contributor_merged_ids_by_name: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_activity: ActivityMapping,
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    contact_merged_id_by_query_string: Mapping[
        str,
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier,
    ],
) -> ExtractedActivity:
    """Transform a synopse project into a MEx activity.

    Args:
        synopse_project: a synopse project
        extracted_primary_source: Extracted report server primary sources
        contact_merged_ids_by_emails: Mapping from LDAP emails to contact IDs
        contributor_merged_ids_by_name: Mapping from person names to contributor IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID
        synopse_activity: synopse activity default values
        synopse_organization_ids_by_query_string: merged organization ids by org name
        contact_merged_id_by_query_string: contact merged identifier lookup by email

    Returns:
        extracted activity
    """
    contact = contact_merged_id_by_query_string[
        synopse_activity.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
    ]
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
