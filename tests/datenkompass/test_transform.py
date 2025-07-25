from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from mex.common.types import Identifier
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.models.item import (
    DatenkompassBibliographicResource,
    DatenkompassResource,
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
from tests.datenkompass.mocked_item_lists import (
    mocked_bmg,
    mocked_datenkompass_activity,
    mocked_merged_activities,
    mocked_merged_bibliographic_resource,
    mocked_merged_contact_point,
    mocked_merged_organizational_units,
    mocked_merged_person,
    mocked_merged_resource,
)


def test_get_contact() -> None:
    responsible_unit_ids = mocked_merged_activities()[0].responsibleUnit
    all_units = {unit.identifier: unit for unit in mocked_merged_organizational_units()}
    result = get_contact(responsible_unit_ids, all_units)

    assert sorted(result) == [
        "a.bsp. unit",
        "e.g. unit",
        "unit@example.org",
    ]


def test_get_resource_contact() -> None:
    item = mocked_merged_resource()[0]
    responsible_unit_ids = cast(
        "list[Identifier]", sorted({*item.contact, *item.unitInCharge})
    )
    all_units = {unit.identifier: unit for unit in mocked_merged_organizational_units()}
    all_contacts = {cp.identifier: cp for cp in mocked_merged_contact_point()}

    result = get_resource_contact(responsible_unit_ids, all_units, all_contacts)

    assert sorted(result) == [
        "a.bsp. unit",
        "contactpoint@example.org",
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


def test_get_datenbank() -> None:
    item = mocked_merged_bibliographic_resource()[0]

    assert get_datenbank(item) == (
        "https://doi.org/10.1234_find_this_first, find_second_a, "
        "find_second_b, https://www.find_third.to"
    )


def test_transform_activities() -> None:
    extracted_and_filtered_merged_activities = mocked_merged_activities()[
        :2
    ]  # item with no BMG filtered out
    merged_organizational_units = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }

    result = transform_activities(
        extracted_and_filtered_merged_activities, merged_organizational_units
    )

    assert result == mocked_datenkompass_activity()


@pytest.mark.usefixtures("mocked_backend_api_connector")
def test_transform_bibliographic_resource() -> None:
    extracted_and_filtered_merged_bibliographic_resource = (
        mocked_merged_bibliographic_resource()
    )
    merged_organizational_units = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }
    person_name_by_id = {
        person.identifier: person.fullName for person in mocked_merged_person()
    }

    result = transform_bibliographic_resources(
        extracted_and_filtered_merged_bibliographic_resource,
        merged_organizational_units,
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


@pytest.mark.usefixtures("mocked_backend_api_connector")
def test_transform_resources() -> None:
    extracted_merged_resource = {
        "Open-Data": [mocked_merged_resource()[0]],
        "Synopse": [mocked_merged_resource()[1]],
    }
    extracted_and_filtered_merged_activities = mocked_merged_activities()[:2]
    bmg_ids = {bmg.identifier for bmg in mocked_bmg()}
    extracted_merged_organizational_units = {
        unit.identifier: unit for unit in mocked_merged_organizational_units()
    }
    extracted_merged_contact_points = {
        cp.identifier: cp for cp in mocked_merged_contact_point()
    }

    result = transform_resources(
        extracted_merged_resource,
        extracted_and_filtered_merged_activities,
        bmg_ids,
        extracted_merged_organizational_units,
        extracted_merged_contact_points,
    )

    assert result == [
        DatenkompassResource(
            voraussetzungen="Frei zugänglich",
            frequenz=None,
            kontakt=[
                "e.g. unit",
                "unit@example.org",
                "a.bsp. unit",
                "contactpoint@example.org",
            ],
            beschreibung="deutsche Beschreibung",
            datenbank="https://doi.org/10.1234_example",
            rechtsgrundlagenbenennung=["has basis", "hat weitere Basis"],
            datennutzungszweckerweitert=["has purpose"],
            schlagwort=["Infektionskrankheiten und -epidemiologie", "word 1", "Wort 2"],
            dk_format=[],
            titel=["some open data resource title"],
            datenhalter="BMG",
            hauptkategorie="Gesundheit",
            unterkategorie=["Public Health"],
            rechtsgrundlage="Ja",
            datenerhalt="Externe Zulieferung",
            status="Stabil",
            datennutzungszweck=["Themenspezifische Auswertung"],
            herausgeber="Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            identifier="openDataResource",
            entityType="MergedResource",
        ),
        DatenkompassResource(
            voraussetzungen="Zugang eingeschränkt",
            frequenz=None,
            kontakt=["a.bsp. unit"],
            beschreibung="n/a",
            datenbank=None,
            rechtsgrundlagenbenennung=[],
            datennutzungszweckerweitert=[],
            schlagwort=["Infektionskrankheiten und -epidemiologie"],
            dk_format=[],
            titel=["some synopse resource title"],
            datenhalter=None,
            hauptkategorie="Gesundheit",
            unterkategorie=["Public Health", "Gesundheitliche Lage"],
            rechtsgrundlage="Nicht bekannt",
            datenerhalt="Externe Zulieferung",
            status="Stabil",
            datennutzungszweck=[
                "Themenspezifische Auswertung",
                "Themenspezifisches Monitoring",
            ],
            herausgeber="Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            identifier="SynopseResource",
            entityType="MergedResource",
        ),
    ]
