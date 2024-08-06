import re
from typing import Any

from mex.common.exceptions import MExError
from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.types import (
    ActivityType,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.ff_projects.models.source import FFProjectsSource

RKI_AZ_TYPES: dict[str, Any] = {}


def get_rki_az_types(rki_azs: str) -> list[ActivityType]:
    """Get the source types for numerical RKI AZs.

    Args:
        rki_azs: numerical AZS

    Returns:
        Source type enums
    """
    try:
        return sorted(
            {
                type_
                for number in re.findall(r"[0-9]{4}", rki_azs)
                for type_ in RKI_AZ_TYPES[number]
            },
            key=lambda s: s.name,
        )
    except KeyError:
        return []


def transform_ff_projects_source_to_extracted_activity(
    ff_projects_source: FFProjectsSource,
    extracted_primary_source: ExtractedPrimarySource,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_id_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    organization_stable_target_id_by_synonyms: dict[str, MergedOrganizationIdentifier],
) -> ExtractedActivity:
    """Transform FF Projects source to an extracted activity.

    Args:
        ff_projects_source: FF Projects source
        extracted_primary_source: Extracted primary_source for FF Projects
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target ID
        unit_stable_target_id_by_synonym: Mapping from unit acronyms and labels
                                          to unit stable target ID
        organization_stable_target_id_by_synonyms: Mapping from organization synonyms
                                                   to organization stable target ID

    Returns:
        Extracted activity for the given projects source
    """
    if rki_oe := ff_projects_source.rki_oe:
        responsible_unit = unit_stable_target_id_by_synonym[rki_oe]
    else:
        raise MExError("missing unit should have been filtered out")

    project_lead = person_stable_target_ids_by_query_string.get(
        ff_projects_source.projektleiter
    )
    funder_or_commissioner = organization_stable_target_id_by_synonyms.get(
        ff_projects_source.zuwendungs_oder_auftraggeber
    )
    return ExtractedActivity(
        title=ff_projects_source.thema_des_projekts,
        activityType=get_rki_az_types(ff_projects_source.rki_az),
        contact=project_lead or responsible_unit,
        involvedPerson=project_lead,
        start=ff_projects_source.laufzeit_von,
        end=ff_projects_source.laufzeit_bis,
        responsibleUnit=responsible_unit,
        identifierInPrimarySource=ff_projects_source.lfd_nr,
        funderOrCommissioner=funder_or_commissioner,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        fundingProgram=ff_projects_source.foerderprogr,
    )
