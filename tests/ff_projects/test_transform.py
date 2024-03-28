import pytest

from mex.common.models import ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import (
    ActivityType,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TemporalEntity,
    TemporalEntityPrecision,
    TextLanguage,
)
from mex.ff_projects.models.source import FFProjectsSource
from mex.ff_projects.transform import (
    get_rki_az_types,
    transform_ff_projects_source_to_extracted_activity,
)


@pytest.mark.parametrize(
    ("rki_azs", "expected_types"),
    [
        ("foo", []),
        ("9999", []),
        (
            "1360",
            [
                ActivityType["CONTRACT_RESEARCH"],
                ActivityType["THIRD_PARTY_FUNDED_PROJECT"],
            ],
        ),
        (
            "(1363)1361",
            [
                ActivityType["CONTRACT_RESEARCH"],
                ActivityType["INTERNATIONAL_PROJECT"],
                ActivityType["THIRD_PARTY_FUNDED_PROJECT"],
            ],
        ),
        (
            "1361   /   9999",
            [],
        ),
    ],
)
def test_get_rki_az_types(rki_azs: str, expected_types: list[ActivityType]) -> None:
    types = get_rki_az_types(rki_azs)

    assert types == expected_types


def test_transform_ff_projects_source_to_extracted_activity(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    organizations_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    person_id = MergedPersonIdentifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": [person_id]}
    unit_id = MergedOrganizationalUnitIdentifier.generate(seed=21)
    unit_stable_target_ids_by_synonym = {"FG99": unit_id}

    laufzeit_bis = TemporalEntity("2019-08-31T23:00:00Z")
    laufzeit_bis.precision = TemporalEntityPrecision.DAY

    laufzeit_von = TemporalEntity("2017-12-31T23:00:00Z")
    laufzeit_von.precision = TemporalEntityPrecision.DAY

    ff_projects_source = FFProjectsSource(
        foerderprogr="Funding",
        kategorie="Entgelt",
        laufzeit_bis=laufzeit_bis,
        laufzeit_cells=("2018-01-01 00:00:00", "2019-09-01 00:00:00"),
        laufzeit_von=laufzeit_von,
        lfd_nr="19",
        projektleiter="Dr Frieda Ficticious",
        rki_az="1364",
        rki_oe="FG99",
        thema_des_projekts="This is a project with a topic.",
        zuwendungs_oder_auftraggeber="Test-Institute",
    )

    extracted_activity = transform_ff_projects_source_to_extracted_activity(
        ff_projects_source,
        extracted_primary_sources["ff-projects"],
        person_stable_target_ids_by_query_string,
        unit_stable_target_ids_by_synonym,
        organizations_stable_target_ids_by_synonym,
    )

    assert extracted_activity.model_dump(exclude_none=True, exclude_defaults=True) == {
        "activityType": ["https://mex.rki.de/item/activity-type-1"],
        "contact": [person_id],
        "end": [laufzeit_bis],
        "funderOrCommissioner": [organization_id],
        "fundingProgram": ["Funding"],
        "hadPrimarySource": extracted_primary_sources["ff-projects"].stableTargetId,
        "identifier": Joker(),
        "identifierInPrimarySource": "19",
        "involvedPerson": [person_id],
        "responsibleUnit": [unit_id],
        "stableTargetId": Joker(),
        "start": [laufzeit_von],
        "title": [
            {"language": TextLanguage.EN, "value": "This is a project with a topic."}
        ],
    }
