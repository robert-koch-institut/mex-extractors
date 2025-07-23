from collections.abc import Hashable

import pytest

from mex.common.models import ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.datscha_web.models.item import DatschaWebItem
from mex.extractors.datscha_web.transform import (
    transform_datscha_web_items_to_mex_activities,
)


@pytest.fixture
def person_stable_target_ids_by_query_string() -> dict[
    Hashable, list[MergedPersonIdentifier]
]:
    return {
        "Coolname, Cordula/ Ausgedacht, Alwina": [
            MergedPersonIdentifier("ID000000001111"),
            MergedPersonIdentifier("ID000000002222"),
        ],
        None: [],
    }


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> dict[
    str, MergedOrganizationalUnitIdentifier
]:
    return {
        "L1": MergedOrganizationalUnitIdentifier("ID000000000033"),
        "FG99": MergedOrganizationalUnitIdentifier("ID000000000044"),
        "Abteilung 2": MergedOrganizationalUnitIdentifier("ID000000000055"),
    }


@pytest.fixture
def organizations_stable_target_ids_by_query_string() -> dict[
    str, MergedOrganizationIdentifier
]:
    return {
        "Fancy Fake Firm & CoKG": MergedOrganizationIdentifier("ID000000000077"),
        "FG99": MergedOrganizationIdentifier("ID000000000884"),
        "Abteilung 2": MergedOrganizationIdentifier("ID000000000039"),
    }


def test_transform_datscha_web_items_to_mex_activities(
    datscha_web_item: DatschaWebItem,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    organizations_stable_target_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    mex_sources = list(
        transform_datscha_web_items_to_mex_activities(
            [datscha_web_item],
            extracted_primary_sources["datscha-web"],
            person_stable_target_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
            organizations_stable_target_ids_by_query_string,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "abstract": [
            {
                "value": "Est quas tempore placeat. Nam explicabo et odit "
                "dignissimos mollitia ipsam. Ea rem molestias "
                "accusamus quaerat id repudiandae. A laudantium sint "
                "doloribus eveniet sit deleniti necessitatibus."
            }
        ],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": ["ID000000001111", "ID000000002222"],
        "externalAssociate": ["ID000000000077"],
        "hadPrimarySource": str(
            extracted_primary_sources["datscha-web"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "17",
        "involvedPerson": ["ID000000001111", "ID000000002222"],
        "involvedUnit": ["ID000000000055"],
        "responsibleUnit": ["ID000000000033", "ID000000000044"],
        "stableTargetId": Joker(),
        "title": [{"value": "Consequuntur atque reiciendis voluptates minus."}],
    }


def test_transform_datscha_web_items_to_mex_activities_without_involved_persons(
    datscha_web_item_without_contributors: DatschaWebItem,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    organizations_stable_target_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    mex_sources = list(
        transform_datscha_web_items_to_mex_activities(
            [datscha_web_item_without_contributors],
            extracted_primary_sources["datscha-web"],
            person_stable_target_ids_by_query_string,
            unit_stable_target_ids_by_synonym,
            organizations_stable_target_ids_by_query_string,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "abstract": [
            {
                "value": "Est quas tempore placeat. Nam explicabo et odit dignissimos "
                "mollitia ipsam. Ea rem molestias accusamus quaerat id "
                "repudiandae. A laudantium sint doloribus eveniet sit deleniti "
                "necessitatibus."
            }
        ],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "contact": ["ID000000000033", "ID000000000044"],
        "externalAssociate": ["ID000000000077"],
        "hadPrimarySource": str(
            extracted_primary_sources["datscha-web"].stableTargetId
        ),
        "identifier": Joker(),
        "identifierInPrimarySource": "92",
        "responsibleUnit": ["ID000000000033", "ID000000000044"],
        "stableTargetId": Joker(),
        "title": [{"value": "Consequuntur atque reiciendis voluptates minus."}],
    }
