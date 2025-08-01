from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from mex.common.types import Identifier
from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedResource,
)
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
)
from mex.extractors.datenkompass.transform import (
    get_contact,
    get_datenbank,
    get_resource_contact,
    get_title,
    get_vocabulary,
    transform_activities,
    transform_bibliographic_resources,
    transform_resources,
)


def test_get_contact(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    responsible_unit_ids = mocked_merged_activities[0].responsibleUnit
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    result = get_contact(responsible_unit_ids, merged_organizational_units_by_id)

    assert sorted(result) == [
        "a.bsp. unit",
        "e.g. unit",
        "unit@example.org",
    ]


def test_get_resource_contact(
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_contact_point: list[MergedContactPoint],
) -> None:
    item = mocked_merged_resource[0]
    responsible_unit_ids = cast(
        "list[Identifier]", sorted({*item.contact, *item.unitInCharge})
    )
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    mocked_merged_contact_point_by_id = {
        cp.identifier: cp for cp in mocked_merged_contact_point
    }

    result = get_resource_contact(
        responsible_unit_ids,
        merged_organizational_units_by_id,
        mocked_merged_contact_point_by_id,
    )

    assert sorted(result) == [
        "a.bsp. unit",
        "contactpoint@example.org",
        "e.g. unit",
        "unit@example.org",
    ]


def test_get_title(mocked_merged_activities: list[MergedActivity]) -> None:
    item = mocked_merged_activities[0]
    result = get_title(item)

    assert result == ["short de", "title no language"]


def test_get_vocabulary() -> None:
    result = get_vocabulary([Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]])
    assert result == ["Infektionskrankheiten und -epidemiologie"]


def test_get_datenbank(
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
) -> None:
    assert get_datenbank(mocked_merged_bibliographic_resource[0]) == (
        "https://doi.org/10.1234_find_this_first, find_second_a, "
        "find_second_b, https://www.find_third.to"
    )


def test_transform_activities(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_datenkompass_activity: list[DatenkompassActivity],
) -> None:
    extracted_and_filtered_merged_activities = mocked_merged_activities[
        :2
    ]  # item with no BMG filtered out
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }

    result = transform_activities(
        extracted_and_filtered_merged_activities, merged_organizational_units_by_id
    )
    assert result == mocked_datenkompass_activity


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_transform_bibliographic_resource(
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_person: list[MergedPerson],
) -> None:
    extracted_and_filtered_merged_bibliographic_resource = (
        mocked_merged_bibliographic_resource
    )
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    person_name_by_id = {
        person.identifier: person.fullName for person in mocked_merged_person
    }

    result = transform_bibliographic_resources(
        extracted_and_filtered_merged_bibliographic_resource,
        merged_organizational_units_by_id,
        person_name_by_id,
    )

    assert result[0].model_dump() == {
        "beschreibung": ["Die Nutzung", "The usage"],
        "kontakt": ["e.g. unit", "unit@example.org"],
        "titel": "title no language, titel en (Pattern, Peppa P. / Pattern, P.P.)",
        "schlagwort": ["short en", "short de"],
        "datenbank": (
            "https://doi.org/10.1234_find_this_first, find_second_a, "
            "find_second_b, https://www.find_third.to"
        ),
        "voraussetzungen": "Frei zugänglich",
        "hauptkategorie": "Gesundheit",
        "unterkategorie": "Public Health",
        "herausgeber": "Robert Koch-Institut",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "dk_format": ["Buch"],
        "identifier": "MergedBibResource1",
    }


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_transform_resources(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_bmg: list[MergedOrganization],
    mocked_merged_contact_point: list[MergedContactPoint],
) -> None:
    extracted_merged_resource = {
        "Open-Data": [mocked_merged_resource[0]],
        "Synopse": [mocked_merged_resource[1]],
    }
    extracted_and_filtered_merged_activities = mocked_merged_activities[:2]
    bmg_ids = {bmg.identifier for bmg in mocked_bmg}
    extracted_merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    extracted_merged_contact_points_by_id = {
        cp.identifier: cp for cp in mocked_merged_contact_point
    }

    result = transform_resources(
        extracted_merged_resource,
        extracted_and_filtered_merged_activities,
        bmg_ids,
        extracted_merged_organizational_units_by_id,
        extracted_merged_contact_points_by_id,
    )

    assert len(result) == 2
    assert result[0].model_dump() == {
        "voraussetzungen": "Frei zugänglich",
        "frequenz": [],
        "kontakt": [
            "e.g. unit",
            "unit@example.org",
            "a.bsp. unit",
            "contactpoint@example.org",
        ],
        "beschreibung": "deutsche Beschreibung",
        "datenbank": "https://doi.org/10.1234_example",
        "rechtsgrundlagenbenennung": ["has basis", "hat weitere Basis"],
        "datennutzungszweckerweitert": ["has purpose"],
        "schlagwort": ["Infektionskrankheiten und -epidemiologie", "word 1", "Wort 2"],
        "dk_format": [],
        "titel": ["some open data resource title"],
        "datenhalter": "BMG",
        "hauptkategorie": "Gesundheit",
        "unterkategorie": ["Public Health"],
        "rechtsgrundlage": "Ja",
        "datenerhalt": "Externe Zulieferung",
        "status": "Stabil",
        "datennutzungszweck": ["Themenspezifische Auswertung"],
        "herausgeber": "Robert Koch-Institut",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "identifier": "openDataResource",
    }

    assert result[1].model_dump() == {
        "voraussetzungen": "Zugang eingeschränkt",
        "frequenz": [],
        "kontakt": ["a.bsp. unit"],
        "beschreibung": "n/a",
        "datenbank": None,
        "rechtsgrundlagenbenennung": [],
        "datennutzungszweckerweitert": [],
        "schlagwort": ["Infektionskrankheiten und -epidemiologie"],
        "dk_format": [],
        "titel": ["some synopse resource title"],
        "datenhalter": None,
        "hauptkategorie": "Gesundheit",
        "unterkategorie": ["Public Health", "Gesundheitliche Lage"],
        "rechtsgrundlage": "Nicht bekannt",
        "datenerhalt": "Externe Zulieferung",
        "status": "Stabil",
        "datennutzungszweck": [
            "Themenspezifische Auswertung",
            "Themenspezifisches Monitoring",
        ],
        "herausgeber": "Robert Koch-Institut",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "identifier": "SynopseResource",
    }
