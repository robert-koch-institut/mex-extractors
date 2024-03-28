from collections.abc import Generator, Iterable

from mex.common.logging import watch
from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.types import TemporalEntity
from mex.rdmo.models.source import RDMOSource


@watch
def transform_rdmo_sources_to_extracted_activities(
    rdmo_sources: Iterable[RDMOSource],
    primary_source: ExtractedPrimarySource,
) -> Generator[ExtractedActivity, None, None]:
    """Transform RDMO sources to extracted activities.

    Args:
        rdmo_sources: RDMO source iterable
        primary_source: MEx primary_source for RDMO

    Returns:
        Generator for ExtractedActivities
    """
    for rdmo_source in rdmo_sources:
        abstract = rdmo_source.description or None
        activity_type = rdmo_source.question_answer_pairs.get("/domain/project/type")
        if name := rdmo_source.question_answer_pairs.get(
            "/domain/project/coordination/contact/name"
        ):
            contact = name
        else:
            continue
        end = (
            TemporalEntity(project_end)
            if (
                project_end := rdmo_source.question_answer_pairs.get(
                    "/domain/project/schedule/project_end"
                )
            )
            else None
        )
        external_associate = rdmo_source.question_answer_pairs.get(
            "/domain/project/partner/external/contact/name"
        )
        funder_or_commissioner = rdmo_source.question_answer_pairs.get(
            "/domain/project/funder/name"
        )

        if name := rdmo_source.question_answer_pairs.get(
            "/domain/project/coordination/name"
        ):
            involved_person = name
        elif name := rdmo_source.question_answer_pairs.get(
            "/domain/project/coordination/deputy/name"
        ):
            involved_person = name
        else:
            involved_person = None

        involved_unit = rdmo_source.question_answer_pairs.get(
            "/domain/project/partner/internal/contact/name"
        )
        if unit := rdmo_source.question_answer_pairs.get(
            "/domain/project/coordination/unit"
        ):
            responsible_unit = unit
        else:
            continue
        short_name = rdmo_source.question_answer_pairs.get(
            "/domain/project/title/acronym"
        )
        start = (
            TemporalEntity(project_start)
            if (
                project_start := rdmo_source.question_answer_pairs.get(
                    "/domain/project/schedule/project_start"
                )
            )
            else None
        )
        if title := rdmo_source.question_answer_pairs.get("/domain/project/title"):
            title = title
        else:
            continue

        yield ExtractedActivity(
            abstract=abstract,
            activityType=activity_type,
            contact=contact,
            end=end,
            externalAssociate=external_associate,
            funderOrCommissioner=funder_or_commissioner,
            hadPrimarySource=primary_source.stableTargetId,
            identifierInPrimarySource=str(rdmo_source.id),
            involvedPerson=involved_person,
            involvedUnit=involved_unit,
            responsibleUnit=responsible_unit,
            shortName=short_name,
            start=start,
            title=title,
            theme="https://mex.rki.de/item/theme-1",
            # TODO: resolve contributor, units and funding organization
            website=None,
        )
