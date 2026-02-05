from mex.common.exceptions import MExError
from mex.common.models import ActivityMapping, ExtractedActivity, ExtractedOrganization
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TemporalEntityPrecision,
    Text,
    YearMonthDay,
)
from mex.extractors.ff_projects.models.source import FFProjectsSource
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load


def transform_ff_projects_source_to_extracted_activity(
    ff_projects_source: FFProjectsSource,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    organization_stable_target_id_by_synonyms: dict[str, MergedOrganizationIdentifier],
    ff_projects_activity: ActivityMapping,
) -> ExtractedActivity:
    """Transform FF Projects source to an extracted activity.

    Args:
        ff_projects_source: FF Projects source
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target ID
        organization_stable_target_id_by_synonyms: Mapping from organization synonyms
                                                   to organization stable target ID
        ff_projects_activity: activity mapping model with default values

    Returns:
        Extracted activity for the given projects source
    """
    if rki_oe := ff_projects_source.rki_oe:
        responsible_unit = [
            unit_id
            for oe in rki_oe.replace("/", ",").split(",")
            if (unit_ids := get_unit_merged_id_by_synonym(oe))
            for unit_id in unit_ids
        ]
    else:
        msg = "missing unit should have been filtered out"
        raise MExError(msg)

    project_lead = [
        sti
        for person in ff_projects_source.projektleiter.replace("/", ",").split(",")
        if person in person_stable_target_ids_by_query_string
        for sti in person_stable_target_ids_by_query_string[person]
    ]
    orgs = ff_projects_source.zuwendungs_oder_auftraggeber.replace("/", ",").split(",")
    funder_or_commissioner: list[MergedOrganizationIdentifier] = []
    for org in orgs:
        if org in (
            ff_projects_activity.funderOrCommissioner[0].mappingRules[1].forValues or ()
        ):
            continue
        funder_or_commissioner = get_or_create_organization(
            orgs,
            organization_stable_target_id_by_synonyms,
        )

    if ff_projects_source.rki_az in (
        ff_projects_activity.activityType[0].mappingRules[0].forValues or ()
    ):
        activity_type = ff_projects_activity.activityType[0].mappingRules[0].setValues
    elif ff_projects_source.rki_az in (
        ff_projects_activity.activityType[0].mappingRules[1].forValues or ()
    ):
        activity_type = ff_projects_activity.activityType[0].mappingRules[1].setValues
    else:
        activity_type = []
    start = (
        [
            YearMonthDay(
                ff_projects_source.laufzeit_von, precision=TemporalEntityPrecision.DAY
            )
        ]
        if ff_projects_source.laufzeit_von
        else []
    )
    end = (
        [
            YearMonthDay(
                ff_projects_source.laufzeit_bis, precision=TemporalEntityPrecision.DAY
            )
        ]
        if ff_projects_source.laufzeit_bis
        else []
    )
    return ExtractedActivity(
        title=ff_projects_source.thema_des_projekts,
        activityType=activity_type,
        contact=project_lead or responsible_unit,
        involvedPerson=project_lead,
        start=start,
        end=end,
        responsibleUnit=responsible_unit,
        identifierInPrimarySource=ff_projects_source.lfd_nr,
        funderOrCommissioner=funder_or_commissioner,
        hadPrimarySource=get_extracted_primary_source_id_by_name("ff-projects"),
        fundingProgram=ff_projects_source.foerderprogr,
    )


def get_or_create_organization(
    orgs: list[str],
    extracted_organizations: dict[str, MergedOrganizationIdentifier],
) -> list[MergedOrganizationIdentifier]:
    """Get organizations merged ids.

    Args:
        orgs: organizations from source
        extracted_organizations: merged organization identifier extracted from wikidata

    Returns:
        list of matched or created organization identifier
    """
    final_organizations: list[MergedOrganizationIdentifier] = []
    for org in orgs:
        extracted_organizations = {
            key.strip().lower(): value for key, value in extracted_organizations.items()
        }
        if org in ("None", "Not applicable"):
            continue
        normalized_org = org.strip().lower()
        if wpo := extracted_organizations.get(normalized_org):
            final_organizations.append(wpo)
            continue
        new_extracted_organization = ExtractedOrganization(
            officialName=[Text(value=org)],
            identifierInPrimarySource=org,
            hadPrimarySource=get_extracted_primary_source_id_by_name("ff-projects"),
        )
        load([new_extracted_organization])
        merged_id = MergedOrganizationIdentifier(
            new_extracted_organization.stableTargetId
        )
        extracted_organizations[normalized_org] = merged_id
        final_organizations.append(merged_id)

    return final_organizations
