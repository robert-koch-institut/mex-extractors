from bs4 import BeautifulSoup

from mex.extractors.confluence_vvt.parse_html import (
    get_row_data_for_all_rows,
    parse_data_html_page,
)
from tests.confluence_vvt.conftest import TEST_DATA_DIR


def test_get_row_data() -> None:
    with (TEST_DATA_DIR / "single_table_data.html").open(encoding="utf-8") as fh:
        table = BeautifulSoup(fh.read(), "html.parser")

    trs_table1 = table.find_all("tr")
    extracted = get_row_data_for_all_rows(trs_table1)

    expected = {
        "Interne Vorgangsnummer (Datenschutz)": {
            "Interne Vorgangsnummer (Datenschutz)": [
                "DS-2024-123; DS-2024-456 (Anpassungen)"
            ],
            "Zuständige/r DatenschutzkoordinatorIn für die Abteilung\n(Achtung: entspricht nicht der/dem DSB!)": [
                "Test Person Name"
            ],
            "(Funktionsbezeichnung der/des zuständigen DatenschutzkoordinatorIn hier einfügen)": [],
            "War eine Datenschutzfolgenabschätzung notwendig? ja/nein": ["Nein"],
        },
        "Ausführliche Beschreibung des Verfahrens / der Verarbeitung / der\n        Studie": "\nLorem ipsum test ipsum blah blah. Lorem ipsum test ipsum blah blah.Lorem ipsum test ipsum blah blah.\n          Lorem ipsum test ipsum blah blah.Lorem ipsum test ipsum blah blah. Lorem ipsum test ipsum blah\n          blah.Lorem ipsum test ipsum blah blah. Lorem ipsum test ipsum blah blah.Lorem ipsum test ipsum blah\n          blah. Lorem ipsum test ipsum blah blah.Lorem ipsum test ipsum blah blah. Lorem ipsum test ipsum blah\n          blah.Lorem ipsum test ipsum blah blah. Lorem ipsum test ipsum blah blah.\n",
        "Verantwortliche(r)StudienleiterIn": {
            "Verantwortliche(r)StudienleiterIn": ["Test Verantwortliche Name"],
            "OE": ["Test Verantwortliche OE"],
            "Abt.": ["Test Verantwortliche Abt"],
            "Tel.": ["Test Verantwortliche Tel"],
        },
        "ggfs. Vertreter der / des Verantwortlichen": {
            "ggfs. Vertreter der / des Verantwortlichen": [
                "Test ggfs. Verantwortliche Name"
            ],
            "OE": ["Test ggfs. Verantwortliche OE"],
            "": ["Test ggfs. Verantwortliche Abt"],
            "Tel.": ["Test ggfs. Verantwortliche Tel"],
        },
        "Mitarbeitende": {
            "Mitarbeitende": [
                "Test Mitarbeitende Name 1",
                "Test Mitarbeitende Name 2",
                "Test Mitarbeitende Name 3",
            ],
            "OE": [
                "Test Mitarbeitende OE 1",
                "Test Mitarbeitende OE 2",
                "Test Mitarbeitende OE 3",
            ],
            "": ["ZIG"],
            "Tel.": [
                "Test Mitarbeitende Tel 1",
                "Test Mitarbeitende Tel 2",
                "Test Mitarbeitende Tel 3",
            ],
        },
        "ggfs. gemeinsam Verantwortliche(r)\n          nach Art. 26 DSGVO (Nennung der Behörde/des Unternehmens inkl. vollständige Adresse UND\n          derKontaktdaten des dortigen Studienleiters / der dortigenStudienleiterin)": {
            "ggfs. gemeinsam Verantwortliche(r)\n          nach Art. 26 DSGVO (Nennung der Behörde/des Unternehmens inkl. vollständige Adresse UND\n          derKontaktdaten des dortigen Studienleiters / der dortigenStudienleiterin)": [
                "Test Name",
                "Test Name",
            ],
            "Tel./Email": ["test@abc.com", "abc@test.com"],
        },
        "ggfs. Name des /der DSB des\n            gemeinsam Verantwortlichen nachArt. 26 DSGVO (inkl. der Kontaktdaten)": "\nDr. Octopus\nEmail:dr-octopus@sea.de\n\nTel.: 01234567890\n",
        "ggfs. Auftragsverarbeiter nach Art. 28 DSGVO": "/",
        "ggfs. Name des /der DSB des\n            Auftragsverarbeiters nach Art. 28 DSGVO (inkl. der Kontaktdaten)": "/",
    }

    assert extracted == expected


def test_parse_data_html_page_mocked(detail_page_data_html: str) -> None:
    page_data = parse_data_html_page(detail_page_data_html)
    assert page_data is not None
    (
        abstract,
        lead_author_names,
        lead_author_oes,
        deputy_author_names,
        deputy_author_oes,
        collaborating_author_names,
        collaborating_author_oes,
        interne_vorgangsnummer,
    ) = page_data

    assert lead_author_oes == ["Test OE 1"]
    assert lead_author_names == ["Test Verantwortliche 1"]
    assert deputy_author_names == ["test ggfs vertreter"]
    assert deputy_author_oes == ["FG99"]
    assert collaborating_author_names == ["Test mitarbeitende 1"]
    assert collaborating_author_oes == ["test OE 1"]
    assert abstract == "test description, test test test, test zwecke des vorhabens"
    assert interne_vorgangsnummer == ["001-002"]
