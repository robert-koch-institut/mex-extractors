from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.transform import (
    get_contact,
    get_title,
    get_vocabulary,
    transform_activities,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_datenkompass_activity,
    mocked_merged_activities,
    mocked_merged_organizational_units,
)


def test_get_contact() -> None:
    responsible_unit_ids = mocked_merged_activities()[0].responsibleUnit
    all_units = mocked_merged_organizational_units()
    result = get_contact(responsible_unit_ids, all_units)

    assert sorted(result) == [
        "a.bsp. unit",
        "e.g. unit",
        "unit@example.org",
    ]


def test_get_title() -> None:
    item = mocked_merged_activities()[0]
    result = get_title(item)

    assert result == ["short de", "title no language"]


def test_get_vocabulary() -> None:
    result = get_vocabulary([Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]])
    assert result == ["Infektionskrankheiten und -epidemiologie"]


def test_transform_activities() -> None:
    extracted_and_filtered_merged_activities = mocked_merged_activities()[
        :2
    ]  # item with no BMG filtered out
    all_units = mocked_merged_organizational_units()

    result = transform_activities(extracted_and_filtered_merged_activities, all_units)

    assert result == mocked_datenkompass_activity()
