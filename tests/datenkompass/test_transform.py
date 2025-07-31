import pytest

from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.models.item import DatenkompassBibliographicResource
from mex.extractors.datenkompass.transform import (
    get_contact,
    get_datenbank,
    get_title,
    get_vocabulary,
    transform_activities,
    transform_bibliographic_resources,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_datenkompass_activity,
    mocked_merged_activities,
    mocked_merged_bibliographic_resource,
    mocked_merged_organizational_units,
    mocked_merged_person,
)


def test_get_contact(mocked_merged_activities: list[MergedActivity]) -> None:
    responsible_unit_ids = mocked_merged_activities()[0].responsibleUnit
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }
    result = get_contact(responsible_unit_ids, merged_organizational_units_by_id)

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


def test_get_datenbank(mocked_merged_activities: list[MergedActivity]) -> None:
    item = mocked_merged_bibliographic_resource()[0]

    assert get_datenbank(item) == (
        "https://doi.org/10.1234_find_this_first, find_second_a, "
        "find_second_b, https://www.find_third.to"
    )


def test_transform_activities(mocked_merged_activities: list[MergedActivity]) -> None:
    extracted_and_filtered_merged_activities = mocked_merged_activities()[
        :2
    ]  # item with no BMG filtered out
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }

    result = transform_activities(
        extracted_and_filtered_merged_activities, merged_organizational_units_by_id
    )

    assert result == mocked_datenkompass_activity()


@pytest.mark.usefixtures("mocked_backend_api_connector")
def test_transform_bibliographic_resource() -> None:
    extracted_and_filtered_merged_bibliographic_resource = (
        mocked_merged_bibliographic_resource()
    )
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }
    person_name_by_id = {
        person.identifier: person.fullName for person in mocked_merged_person()
    }

    result = transform_bibliographic_resources(
        extracted_and_filtered_merged_bibliographic_resource,
        merged_organizational_units_by_id,
        person_name_by_id,
    )

    assert result == [
        DatenkompassBibliographicResource(
            beschreibung=["Die Nutzung", "The usage"],
            kontakt=["e.g. unit", "unit@example.org"],
            titel="title no language, titel en (Pattern, Peppa P. / Pattern, P.P.)",
            schlagwort=["short en", "short de"],
            datenbank=(
                "https://doi.org/10.1234_find_this_first, find_second_a, "
                "find_second_b, https://www.find_third.to"
            ),
            voraussetzungen="Frei zugänglich",
            hauptkategorie="Gesundheit",
            unterkategorie="Public Health",
            herausgeber="Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            dk_format=["Buch"],
            identifier="MergedBibResource1",
            entityType="MergedBibliographicResource",
        ),
    ]
