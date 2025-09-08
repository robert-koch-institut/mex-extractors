import pytest

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedResource,
)
from mex.common.types import Text
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
)
from mex.extractors.datenkompass.transform import (
    fix_quotes,
    get_abstract_or_description,
    get_datenbank,
    get_email,
    get_german_text,
    get_german_vocabulary,
    get_resource_email,
    get_title,
    get_unit_shortname,
    transform_activities,
    transform_bibliographic_resources,
    transform_resources,
)


def test_fix_quotes() -> None:
    test_str = '"Outer "double quotes" removed, inner "double quotes" replaced."'

    assert fix_quotes(test_str) == (
        "Outer 'double quotes' removed, inner 'double quotes' replaced."
    )


def test_get_unit_shortname(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    responsible_unit_ids = mocked_merged_activities[0].responsibleUnit
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    result = get_unit_shortname(responsible_unit_ids, merged_organizational_units_by_id)

    assert sorted(result) == [
        "a.bsp. unit",
        "e.g. unit",
    ]


def test_get_email(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    responsible_unit_ids = mocked_merged_activities[0].responsibleUnit
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    result = get_email(responsible_unit_ids, merged_organizational_units_by_id)

    assert result == "unit@example.org"


def test_get_resource_email(
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_contact_point: list[MergedContactPoint],
) -> None:
    item = mocked_merged_resource[0]
    responsible_unit_ids = item.contact
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    mocked_merged_contact_point_by_id = {
        cp.identifier: cp for cp in mocked_merged_contact_point
    }

    result = get_resource_email(
        responsible_unit_ids,
        merged_organizational_units_by_id,
        mocked_merged_contact_point_by_id,
    )

    assert result == "unit@example.org"


@pytest.mark.parametrize(
    ("text_entries", "expected"),
    [
        (
            [
                Text(value='deu "1"."', language="de"),
                Text(value="deu 2", language="de"),
                Text(value='"eng"li"sh"', language="en"),
                Text(value="null", language=None),
            ],
            [
                "deu '1'.",
                "deu 2",
            ],
        ),
        (
            [
                Text(value='"eng"li"sh"', language="en"),
                Text(value="null", language=None),
            ],
            [
                "eng'li'sh",
                "null",
            ],
        ),
    ],
    ids=["german and other languages mixed", "only non-german entries"],
)
def test_get_german_text(text_entries: list[Text], expected: list[str]) -> None:
    assert get_german_text(text_entries) == expected


def test_get_title(mocked_merged_activities: list[MergedActivity]) -> None:
    item = mocked_merged_activities[0]
    result = get_title(item)

    assert result == ["short de", "title 'Act' no language", "title en"]


def test_get_german_vocabulary() -> None:
    result = get_german_vocabulary([Theme["INFECTIOUS_DISEASES_AND_EPIDEMIOLOGY"]])
    assert result == ["Infektionskrankheiten und -epidemiologie"]


def test_get_datenbank(
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
) -> None:
    assert get_datenbank(mocked_merged_bibliographic_resource[0]) == (
        "https://doi.org/10.1234_find_this"
    )


def test_get_abstract_or_description() -> None:
    test_abstracts = [
        Text(value="This is a <b>text</b>", language="de"),
        Text(value='with a <a href="https://link.url">Link text</a>.', language="de"),
    ]
    delimiter = "; "
    assert get_abstract_or_description(test_abstracts, delimiter) == (
        "This is a <b>text</b>; with a https://link.url."
    )


def test_transform_activities(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_datenkompass_activity: list[DatenkompassActivity],
) -> None:
    extracted_and_filtered_merged_activities = mocked_merged_activities[
        :2
    ]  # item with wrong organization filtered out
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
        person.identifier: person.fullName[0] for person in mocked_merged_person
    }

    result = transform_bibliographic_resources(
        extracted_and_filtered_merged_bibliographic_resource,
        merged_organizational_units_by_id,
        person_name_by_id,
    )

    assert result[0].model_dump() == {
        "kontakt": "unit@example.org",
        "beschreibung": "Buch. Die Nutzung",
        "organisationseinheit": ["e.g. unit"],
        "titel": (
            "title 'BibRes' no language, title en (Pattern, Peppa P. / "
            "Pattern, Peppa P. / Pattern, Peppa P. / et al.)"
        ),
        "schlagwort": ["short en", "short de"],
        "datenbank": "https://doi.org/10.1234_find_this",
        "rechtsgrundlagen_benennung": "Nicht zutreffend",
        "datennutzungszweck_erweitert": "Nicht zutreffend",
        "voraussetzungen": "Frei zugänglich",
        "datenhalter": "Robert Koch-Institut",
        "frequenz": "Nicht zutreffend",
        "hauptkategorie": "Gesundheit",
        "unterkategorie": "Einflussfaktoren auf die Gesundheit",
        "herausgeber": "RKI - Robert Koch-Institut",
        "datenerhalt": "Abruf über eine externe Internetseite oder eine Datenbank",
        "status": "Stabil",
        "datennutzungszweck": "Sonstige",
        "rechtsgrundlage": "Nicht zutreffend",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "dk_format": "Sonstiges",
        "identifier": "MergedBibResource1",
    }


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_transform_resources(
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_contact_point: list[MergedContactPoint],
) -> None:
    fetched_merged_resource = {
        "open-data": [mocked_merged_resource[0]],
        "report-server": [mocked_merged_resource[1]],
    }
    fetched_merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    fetched_merged_contact_points_by_id = {
        cp.identifier: cp for cp in mocked_merged_contact_point
    }

    result = transform_resources(
        fetched_merged_resource,
        fetched_merged_organizational_units_by_id,
        fetched_merged_contact_points_by_id,
    )

    assert len(result) == 2
    assert result[0].model_dump() == {
        "voraussetzungen": "Frei zugänglich",
        "frequenz": [],
        "kontakt": "unit@example.org",
        "organisationseinheit": ["e.g. unit"],
        "beschreibung": "deutsche Beschreibung http://mit.link.",
        "datenbank": "https://doi.org/10.1234_example",
        "rechtsgrundlagen_benennung": ["has basis", "hat weitere Basis"],
        "datennutzungszweck_erweitert": ["has purpose"],
        "schlagwort": ["Infektionskrankheiten und -epidemiologie", "word 1", "Wort 2"],
        "dk_format": "Sonstiges",
        "titel": ["some open data resource title"],
        "datenhalter": "Robert Koch-Institut",
        "hauptkategorie": "Gesundheit",
        "unterkategorie": "Einflussfaktoren auf die Gesundheit",
        "rechtsgrundlage": "Nicht zutreffend",
        "datenerhalt": "Externe Zulieferung",
        "status": "Stabil",
        "datennutzungszweck": ["Themenspezifische Auswertung"],
        "herausgeber": "RKI - Robert Koch-Institut",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "identifier": "openDataResource",
    }

    assert result[1].model_dump() == {
        "voraussetzungen": "Zugang eingeschränkt",
        "frequenz": [],
        "kontakt": None,
        "organisationseinheit": ["a.bsp. unit"],
        "beschreibung": "n/a",
        "datenbank": None,
        "rechtsgrundlagen_benennung": [],
        "datennutzungszweck_erweitert": [],
        "schlagwort": ["Infektionskrankheiten und -epidemiologie"],
        "dk_format": "Sonstiges",
        "titel": ["some synopse resource title"],
        "datenhalter": "Robert Koch-Institut",
        "hauptkategorie": "Gesundheit",
        "unterkategorie": "Einflussfaktoren auf die Gesundheit",
        "rechtsgrundlage": "Nicht zutreffend",
        "datenerhalt": "Externe Zulieferung",
        "status": "Stabil",
        "datennutzungszweck": [
            "Themenspezifische Auswertung",
            "Themenspezifisches Monitoring",
        ],
        "herausgeber": "RKI - Robert Koch-Institut",
        "kommentar": (
            "Link zum Metadatensatz im RKI Metadatenkatalog wird "
            "voraussichtlich Ende 2025 verfügbar sein."
        ),
        "identifier": "SynopseResource",
    }
