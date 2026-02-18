import pytest

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedResource,
)
from mex.common.types import (
    TemporalEntity,
    Text,
    Year,
    YearMonth,
    YearMonthDay,
    YearMonthDayTime,
)
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
)
from mex.extractors.datenkompass.models.mapping import (
    DatenkompassMapping,
    DatenkompassMappingField,
    MappingOrFilterRule,
)
from mex.extractors.datenkompass.transform import (
    built_string_shorter_than_limit,
    fix_quotes,
    format_temporal_entity,
    get_abstract_or_description,
    get_datenbank,
    get_email,
    get_german_text,
    get_german_vocabulary,
    get_resource_email,
    get_title,
    get_unit_shortname,
    handle_setval,
    mapping_lookup_default,
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
    delim = "; "
    result = get_unit_shortname(
        responsible_unit_ids,
        merged_organizational_units_by_id,
        delim,
    )

    assert result == "C1; FG 99"


def test_get_email(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    responsible_unit_ids = mocked_merged_activities[1].responsibleUnit
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    result = get_email(responsible_unit_ids, merged_organizational_units_by_id)

    assert result == "pu@example.com"


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

    assert result == "fg@example.com"


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
    assert get_abstract_or_description([], delimiter) == ""


def test_mapping_lookup_default(mocked_activity_mapping: DatenkompassMapping) -> None:
    model = DatenkompassActivity

    result = mapping_lookup_default(model, mocked_activity_mapping)
    assert result == {
        "beschreibung": DatenkompassMappingField(
            fieldInTarget="Beschreibung",
            fieldInMEx="abstract",
            mappingRules=[
                MappingOrFilterRule(
                    setValues=["Es handelt. "],
                )
            ],
        ),
        "datenerhalt": DatenkompassMappingField(
            fieldInTarget="Weg des Datenerhalts",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Datenerhalt",
                )
            ],
        ),
        "datenhalter": DatenkompassMappingField(
            fieldInTarget="Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Datenhalter",
                )
            ],
        ),
        "datennutzungszweck": DatenkompassMappingField(
            fieldInTarget="Datennutzungszweck",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Datennutzungszweck",
                )
            ],
        ),
        "dk_format": DatenkompassMappingField(
            fieldInTarget="Format der Daten",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Format",
                )
            ],
        ),
        "frequenz": DatenkompassMappingField(
            fieldInTarget="Frequenz der Aktualisierung",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Frequenz",
                )
            ],
        ),
        "hauptkategorie": DatenkompassMappingField(
            fieldInTarget="Hauptkategorie",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Hauptkategorie",
                )
            ],
        ),
        "herausgeber": DatenkompassMappingField(
            fieldInTarget="Herausgeber",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Herausgeber",
                )
            ],
        ),
        "kommentar": DatenkompassMappingField(
            fieldInTarget="Kommentar",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Kommentar",
                )
            ],
        ),
        "rechtsgrundlage": DatenkompassMappingField(
            fieldInTarget="Rechtsgrundlage für die Zugangseröffnung",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Rechtsgrundlage",
                )
            ],
        ),
        "status": DatenkompassMappingField(
            fieldInTarget="Status (planbare Verfügbarkeit der Daten)",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Status",
                )
            ],
        ),
        "unterkategorie": DatenkompassMappingField(
            fieldInTarget="Unterkategorie",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Unterkategorie",
                )
            ],
        ),
        "voraussetzungen": DatenkompassMappingField(
            fieldInTarget="Formelle Voraussetzungen für den Datenerhalt",
            mappingRules=[
                MappingOrFilterRule(
                    setValues="Voraussetzungen",
                )
            ],
        ),
    }


@pytest.mark.parametrize(
    ("input_value", "expected_output"),
    [
        (["a", "b", "c"], "a; b; c"),
        (["a"], "a"),
        ("abc", "abc"),
    ],
    ids=["3 items list", "1 item list", "string"],
)
def test_handle_setval_valid(
    input_value: list[str] | str, expected_output: str
) -> None:
    assert handle_setval(input_value) == expected_output


def test_handle_setval_none_raises_error() -> None:
    with pytest.raises(ValueError, match=r"no default value set in mapping."):
        handle_setval(None)


@pytest.mark.parametrize(
    ("input_value", "expected_output"),
    [
        (["aaa", "b", "c", "dd", "ee"], "aaa||dd"),
        (["aa", "dd", "ee"], "aa||dd||ee"),
        (["abcabcalsdhldgh", "y", "http://", "https://", "www.b"], ""),
        ([], ""),
    ],
    ids=["filter for length", "last word just fits", "all filtered out", "empty input"],
)
def test_built_string_shorter_than_limit(
    input_value: list[str | None], expected_output: str
) -> None:
    result = built_string_shorter_than_limit(
        input_value,
        "||",
        2,
        10,
    )
    assert result == expected_output


@pytest.mark.parametrize(
    ("input_value", "expected_output"),
    [
        (YearMonthDayTime("1970-02-15T16:20:30"), "15.02.1970"),
        (YearMonthDay("1971-03-16"), "16.03.1971"),
        (YearMonth("1972-04"), "04.1972"),
        (Year("1975"), "1975"),
        (None, None),
    ],
    ids=["YearMonthDayTime", "YearMonthDay", "YearMonth", "Year", "None"],
)
def test_format_temporal_entity(
    input_value: TemporalEntity, expected_output: str
) -> None:
    assert format_temporal_entity(input_value) == expected_output


def test_transform_activities(
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_datenkompass_activity: list[DatenkompassActivity],
    mocked_activity_mapping: DatenkompassMapping,
) -> None:
    datenkompass_filtered_merged_activities = mocked_merged_activities[
        :2
    ]  # item with wrong organization filtered out
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    result = transform_activities(
        datenkompass_filtered_merged_activities,
        merged_organizational_units_by_id,
        mocked_activity_mapping,
    )
    assert result == mocked_datenkompass_activity


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_transform_bibliographic_resource(
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_person: list[MergedPerson],
    mocked_bibliographic_resource_mapping: DatenkompassMapping,
) -> None:
    merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    datenkompass_person_str_by_id = {
        person.identifier: person.fullName[0] for person in mocked_merged_person
    }

    result = transform_bibliographic_resources(
        mocked_merged_bibliographic_resource,
        merged_organizational_units_by_id,
        datenkompass_person_str_by_id,
        mocked_bibliographic_resource_mapping,
    )

    assert result[0].model_dump() == {
        "kontakt": "pu@example.com",
        "beschreibung": "Buch. Die Nutzung",
        "organisationseinheit": "PRNT",
        "titel": (
            "title 'BibRes' no language, title en (Pattern, Peppa P. / "
            "Pattern, Peppa P. / Pattern, Peppa P. / et al.)"
        ),
        "schlagwort": "short en; short de",
        "datenbank": "https://doi.org/10.1234_find_this",
        "rechtsgrundlagen_benennung": "Rechtsgrundlage",
        "datennutzungszweck_erweitert": "Datennutzungszweck",
        "voraussetzungen": "OPEN",
        "datenhalter": "Datenhalter",
        "frequenz": "Frequenz",
        "hauptkategorie": "Hauptkategorie",
        "unterkategorie": "Unterkategorie",
        "herausgeber": "Herausgeber",
        "datenerhalt": "Datenerhalt",
        "status": "Status",
        "datennutzungszweck": "Datennutzungszweck",
        "rechtsgrundlage": "Rechtsgrundlage",
        "kommentar": "Kommentar",
        "dk_format": "Format",
        "identifier": "MergedBibResource1",
    }


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_transform_resources(
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_contact_point: list[MergedContactPoint],
) -> None:
    merged_resources_by_primary_source_by_unit = {
        "PRNT": {"relevant primary source": [mocked_merged_resource[0]]},
        "FG 99": {"relevant primary source": [mocked_merged_resource[1]]},
    }
    datenkompass_merged_organizational_units_by_id = {
        unit.identifier: unit for unit in mocked_merged_organizational_units
    }
    datenkompass_merged_contact_points_by_id = {
        cp.identifier: cp for cp in mocked_merged_contact_point
    }

    result = transform_resources(
        merged_resources_by_primary_source_by_unit,
        datenkompass_merged_organizational_units_by_id,
        datenkompass_merged_contact_points_by_id,
    )

    assert len(result) == 2
    assert result["PRNT"]["relevant primary source"][0].model_dump() == {
        "voraussetzungen": "OPEN",
        "frequenz": None,
        "kontakt": "fg@example.com",
        "organisationseinheit": "C1",
        "startdatum": "01.01.1970",
        "beschreibung": "deutsche Beschreibung http://mit.link.",
        "datenbank": "https://doi.org/10.1234_example",
        "rechtsgrundlagen_benennung": "has basis; hat weitere Basis",
        "datennutzungszweck_erweitert": "has purpose",
        "schlagwort": "Infektionskrankheiten und -epidemiologie; word 1",  # 'Wort 2' cut
        "dk_format": "Format",
        "titel": "Resource with unit C1",
        "datenhalter": "Datenhalter",
        "hauptkategorie": "Hauptkategorie",
        "unterkategorie": "Unterkategorie",
        "rechtsgrundlage": "Rechtsgrundlage",
        "datenerhalt": "Datenerhalt",
        "status": "Status",
        "datennutzungszweck": "TA; TM",
        "herausgeber": "Herausgeber",
        "kommentar": "Kommentar",
        "identifier": "IdentifierC1Resource",
    }

    assert result["FG 99"]["relevant primary source"][0].model_dump() == {
        "voraussetzungen": "RESTRICTED",
        "frequenz": None,
        "kontakt": None,
        "organisationseinheit": "FG 99",
        "startdatum": None,
        "beschreibung": "n/a",
        "datenbank": None,
        "rechtsgrundlagen_benennung": None,
        "datennutzungszweck_erweitert": None,
        "schlagwort": "Infektionskrankheiten und -epidemiologie",
        "dk_format": "Format",
        "titel": "Resource with unit FG 99",
        "datenhalter": "Datenhalter",
        "hauptkategorie": "Hauptkategorie",
        "unterkategorie": "Unterkategorie",
        "rechtsgrundlage": "Rechtsgrundlage",
        "datenerhalt": "Datenerhalt",
        "status": "Status",
        "datennutzungszweck": "TA; TM",
        "herausgeber": "Herausgeber",
        "kommentar": "Kommentar",
        "identifier": "IdentifierFG99Resource",
    }
