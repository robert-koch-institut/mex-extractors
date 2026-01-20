
from collections.abc import Iterable
import pytest
from pytest import MonkeyPatch

from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
)
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.blueant.transform import (
    transform_blueant_sources_to_extracted_activities,
)
from mex.extractors.organigram.helpers import resolve_organizational_unit_with_fallback
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_blueant_sources_to_extracted_activities(
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    blueant_activity: ActivityMapping,
) -> None:
    stable_target_ids_by_employee_id = {
        "person-567": [
            MergedPersonIdentifier.generate(seed=99),
        ],
    }
    mex_sources = list(
        transform_blueant_sources_to_extracted_activities(
            [blueant_source, blueant_source_without_leader],
            stable_target_ids_by_employee_id,
            blueant_activity,
            {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(seed=42)},
        )
    )
    assert len(mex_sources) == 2
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [str(Identifier.generate(seed=99))],
        "responsibleUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "funderOrCommissioner": [str(Identifier.generate(seed=42))],
        "identifier": Joker(),
        "identifierInPrimarySource": "00123",
        "involvedPerson": ["bFQoRhcVH5DHV1"],
        "title": [{"value": "Prototype Space Rocket", "language": TextLanguage.EN}],
        "start": ["2019-01-07"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("blueant")),
        "stableTargetId": Joker(),
    }




@pytest.mark.usefixtures("mocked_wikidata")
def test_resolve_organizational_unit_with_fallback(
    monkeypatch:MonkeyPatch,
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    blueant_source_with_involved_employee: BlueAntSource,
    blueant_activity: ActivityMapping,
) -> None:
    stable_target_ids_by_employee_id = {
        "person-567": [
            MergedPersonIdentifier.generate(seed=99),
        ],
        "person-789":[
            MergedPersonIdentifier.generate(seed=99),
        ],
    }
    mocked_unit_id = MergedOrganizationIdentifier.generate(seed=42)

    mocked_synonyms = {
        "C1": [mocked_unit_id],
        "C1 Sub-Unit": [mocked_unit_id],
    }

    monkeypatch.setattr(
        "mex.extractors.organigram.helpers._get_cached_unit_merged_ids_by_synonyms",
        lambda: mocked_synonyms,
    )

    def ldap_mock(employee_ids: set[str]) -> set[str]:
        mapping = {
            "person-567": "C1",
            "person-789": "C2",
        }
        return {mapping[eid] for eid in employee_ids if eid in mapping}


    monkeypatch.setattr(
        "mex.extractors.organigram.helpers.get_ldap_units_for_employee_ids",
        ldap_mock,
    )


    mex_sources = transform_blueant_sources_to_extracted_activities(
        [blueant_source, blueant_source_without_leader, blueant_source_with_involved_employee],
        stable_target_ids_by_employee_id,
        blueant_activity,
        {"Robert Koch-Institut": mocked_unit_id},
    )

    assert len(mex_sources) == 3

    # matched department with project leader
    assert mex_sources[0].responsibleUnit == [mocked_unit_id]
    assert mex_sources[0].contact == stable_target_ids_by_employee_id["person-567"]

    # matched department without leader
    assert mex_sources[1].responsibleUnit == [mocked_unit_id]

    # outdated department -> fallback via projectLeaderId
    assert mex_sources[2].responsibleUnit == [mocked_unit_id]
    assert mex_sources[2].contact == stable_target_ids_by_employee_id["person-567"]


