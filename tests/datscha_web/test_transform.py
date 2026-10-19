from typing import TYPE_CHECKING

import pytest

from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.datscha_web.transform import (
    transform_datscha_web_items_to_mex_activities,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Hashable

    from mex.common.models import ExtractedOrganizationalUnit
    from mex.extractors.datscha_web.models.item import DatschaWebItem


@pytest.fixture
def person_stable_target_ids_by_query_string() -> dict[
    Hashable, list[MergedPersonIdentifier]
]:
    return {
        "Coolname, Cordula/ Ausgedacht, Alwina": [
            MergedPersonIdentifier("ID000000001111"),
            MergedPersonIdentifier("ID000000002222"),
        ],
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


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_datscha_web_items_to_mex_activities(
    datscha_web_item: DatschaWebItem,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    organizations_stable_target_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    mex_sources = transform_datscha_web_items_to_mex_activities(
        [datscha_web_item],
        person_stable_target_ids_by_query_string,
        organizations_stable_target_ids_by_query_string,
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
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("datscha-web")),
        "identifier": "hsbBCvjXGidJC8mpvzYWGm",
        "identifierInPrimarySource": "17",
        "involvedPerson": ["ID000000001111", "ID000000002222"],
        "involvedUnit": [
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId
        ],
        "responsibleUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId,
        ],
        "stableTargetId": "XVVpiqQNkBpwvra5C8Njf",
        "title": [{"value": "Consequuntur atque reiciendis voluptates minus."}],
    }


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_datscha_web_items_to_mex_activities_without_involved_persons(
    datscha_web_item_without_contributors: DatschaWebItem,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    organizations_stable_target_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    mex_sources = transform_datscha_web_items_to_mex_activities(
        [datscha_web_item_without_contributors],
        person_stable_target_ids_by_query_string,
        organizations_stable_target_ids_by_query_string,
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
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId,
        ],
        "externalAssociate": ["ID000000000077"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("datscha-web")),
        "identifier": "fKPG6M0s5Rohg4SCcTJPRQ",
        "identifierInPrimarySource": "92",
        "responsibleUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId,
        ],
        "stableTargetId": "89lJllvnzoLz9YvPSCDFM",
        "title": [{"value": "Consequuntur atque reiciendis voluptates minus."}],
    }
