import pytest
from pytest import MonkeyPatch

import mex.extractors.datenkompass.transform as transform_module
from mex.common.exceptions import MExError
from mex.common.models import MergedOrganization
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.transform import (
    check_datenhalter,
    get_contact,
    get_title,
    get_vocabulary,
    transform_activities,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_bmg,
    mocked_datenkompass_activity,
    mocked_merged_activities,
    mocked_merged_organizational_units,
)


def test_get_contact() -> None:
    responsible_unit_ids = mocked_merged_activities()[0].responsibleUnit
    all_units = mocked_merged_organizational_units()
    result = get_contact(responsible_unit_ids, all_units)

    assert result == [
        "a.bsp. unit",
        "e.g. unit",
        "unit@example.org",
    ]


def test_get_title() -> None:
    item = mocked_merged_activities()[0]
    result = get_title(item)

    assert result == ["short de", "title no language"]


def test_get_vocabulary() -> None:
    result = get_vocabulary([Theme["PUBLIC_HEALTH"]])
    assert result == ["Public Health"]


def test_check_datenhalter() -> None:
    bmg_ids = [bmg.identifier for bmg in mocked_bmg()]

    assert (
        check_datenhalter(
            bmg_ids,
            mocked_merged_activities()[0].funderOrCommissioner,  # has bmg id
        )
        == "BMG"
    )

    try:
        check_datenhalter(
            bmg_ids,
            mocked_merged_activities()[2].funderOrCommissioner,  # no bmg id
        )
    except MExError:
        pytest.raises(MExError, match="Funder or Commissioner is not BMG!")


def test_transform_activities(monkeypatch: MonkeyPatch) -> None:
    def fake_get_merged_items(
        query_string: str,  # noqa: ARG001
        entity_type: list[str],  # noqa: ARG001
        had_primary_source: None,  # noqa: ARG001
    ) -> list[MergedOrganization]:
        return mocked_bmg()

    monkeypatch.setattr(transform_module, "get_merged_items", fake_get_merged_items)

    extracted_and_filtered_merged_activities = mocked_merged_activities()[:2]
    all_units = mocked_merged_organizational_units()

    result = transform_activities(extracted_and_filtered_merged_activities, all_units)

    assert result == mocked_datenkompass_activity()
