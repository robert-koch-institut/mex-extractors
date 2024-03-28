import pytest
from pytz import timezone

from mex.common.models import ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    TemporalEntity,
    TemporalEntityPrecision,
    TextLanguage,
    Theme,
)
from mex.international_projects.extract import extract_international_projects_sources
from mex.international_projects.transform import (
    get_theme_for_activity_or_topic,
    transform_international_projects_sources_to_extracted_activities,
)


def test_transform_international_projects_source_to_mex_source(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    unit_stable_target_ids_by_synonym: dict[str, Identifier],
) -> None:
    end = TemporalEntity(2021, 12, 30, 23, 0, tzinfo=timezone("UTC"))
    end.precision = TemporalEntityPrecision.DAY

    start = TemporalEntity(2021, 7, 26, 23, 0, tzinfo=timezone("UTC"))
    start.precision = TemporalEntityPrecision.DAY

    organization_id = Identifier.generate(seed=44)
    funding_source_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    partner_organizations_stable_target_ids_by_synonym = {"WHO": organization_id}
    person_id = Identifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": [person_id]}
    unit_id = Identifier.generate(seed=21)
    unit_stable_target_ids_by_synonym = {"FG99": unit_id}

    international_projects_sources = extract_international_projects_sources()

    extracted_activities = list(
        transform_international_projects_sources_to_extracted_activities(
            international_projects_sources,
            extracted_primary_sources["international-projects"],
            person_stable_target_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
            funding_source_stable_target_ids_by_synonym,
            partner_organizations_stable_target_ids_by_synonym,
        )
    )

    expected = {
        "identifier": Joker(),
        "hadPrimarySource": extracted_primary_sources[
            "international-projects"
        ].stableTargetId,
        "identifierInPrimarySource": "0000-1000",
        "stableTargetId": Joker(),
        "activityType": [
            "https://mex.rki.de/item/activity-type-2",
            "https://mex.rki.de/item/activity-type-1",
        ],
        "alternativeTitle": [{"value": "testAAbr"}],
        "contact": [
            person_id,
            unit_id,
        ],
        "end": [end],
        "externalAssociate": [organization_id],
        "funderOrCommissioner": [organization_id],
        "involvedPerson": [person_id],
        "involvedUnit": [unit_id],
        "responsibleUnit": [unit_id],
        "shortName": [{"value": "testAAbr"}],
        "start": [start],
        "theme": ["https://mex.rki.de/item/theme-27"],
        "title": [
            {"value": "This is a test project full title", "language": TextLanguage.EN}
        ],
    }

    assert (
        extracted_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.parametrize(
    ("activity1", "activity2", "topic1", "topic2", "expected_themes"),
    [
        ("Crisis management", "", "", "", [Theme["PUBLIC_HEALTH"]]),
        ("", "", "", "Other", [Theme["PUBLIC_HEALTH"]]),
        (
            "Capacity building including trainings",
            "",
            "Crisis management",
            "",
            [Theme["PUBLIC_HEALTH"]],
        ),
        (
            "Non-communicable diseases",
            "",
            "Crisis management",
            "",
            [Theme["PUBLIC_HEALTH"], Theme["NON_COMMUNICABLE_DISEASE"]],
        ),
        (
            "",
            "Laboratory diagnostics",
            "",
            "Non-communicable diseases",
            [Theme["NON_COMMUNICABLE_DISEASE"], Theme["LABORATORY"]],
        ),
    ],
)
def test_get_theme_for_activity_or_topic(
    activity1: str,
    activity2: str,
    topic1: str,
    topic2: str,
    expected_themes: list[Theme],
) -> None:
    theme = get_theme_for_activity_or_topic(activity1, activity2, topic1, topic2)

    assert len(theme) == len(expected_themes)
    assert theme == sorted(list(expected_themes), key=lambda x: x.name)
