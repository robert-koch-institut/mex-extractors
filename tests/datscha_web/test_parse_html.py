from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from mex.common.exceptions import MExError
from mex.extractors.datscha_web.parse_html import (
    parse_detail_block,
    parse_item_urls_from_overview_html,
    parse_single_item_html,
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"


def test_parse_item_urls_from_overview_html() -> None:
    with open(TEST_DATA_DIR / "overview.htm", "rb") as handle:
        urls = parse_item_urls_from_overview_html(handle.read(), "https://datscha/")
    assert urls == [
        "https://datscha/verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        "https://datscha/verzeichnis_detail.php?vavs_id=46&option=Anzeigen",
    ]


def test_parse_single_item_html() -> None:
    with open(TEST_DATA_DIR / "single_entry.htm", "rb") as handle:
        details = parse_single_item_html(
            handle.read(),
            "verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        )
    expected_details = {
        "item_id": 39,
        "Bezeichnung der Verarbeitungstätigkeit": '"Qualitätssicherung Gesundheitsstudie"',
        "Version": "1.0",
        "Kurzbeschreibung": "Datenqualitätsanalysen",
        "Zentrale Stelle für die Verarbeitung": "Abteilung Team",
        "Zweckbestimmung der Datenverarbeitung": (
            "Im Interesse steht, ob die Untersuchungen und Befragungen der "
            "Teilnehmenden der Gesundheitsstudie vollständig durchgeführt wurden und "
            "alle zu erwartenden Daten vorliegen."
        ),
        "Rechtsgrundlage": "Es besteht mit dem Projektträger der Gesundheitsstudie ein Vertrag.",
        "Herkunft der Daten": "Die Daten werden in den Studienzentren erhoben.",
        "Betroffene Personen": "Die Teilnehmenden kommen aus den Studienregionen.",
        "Daten oder Datenkategorien": "Studien-/ Gesundheitsdaten (Alter, Geschlecht, Untersuchungsvariablen).",
        "Löschfrist der Daten": "Laut Vorgaben sind die Daten nach Erhalt zu löschen.",
        "Zugriffsberechtigte": "Die Mitarbeitenden der externen Qualitätssicherung in der Abteilung.",
        "Gemeinsames/Verbundenes Verfahren": "nein",
        "Abrufverfahren": "nein",
        "Verantwortlicher für das Abrufverfahren": "None",
        "EU-Verfahren": "nein",
        "Auftragsverarbeitung (Art.\xa028\xa0DSGVO)": "nein",
        "Auftragsverarbeiter": "None",
        "Datenverarbeitung (Übermittlung/Offenlegung)": "nein",
        "Art der verarbeiteten (übermittelten/offengelegten) Daten": "None",
        "Datenübermittlung in Drittstaat": "nein",
        "Name des Drittstaats": "None",
        "Empfänger der Daten im Drittstaat": "None",
        "Hard- und Software": (
            "Datenspeicherung, Analysen, Ergebnisberichte erfolgen an den "
            "Dienstrechnern."
        ),
        "Schutzmaßnahmen (Art.\xa032\xa0DSGVO)": "Siehe DS-Fragenkatalog",
        "Zugriffskontrolle": "Siehe DS-Fragenkatalog",
        "Weitergabekontrolle": "Siehe DS-Fragenkatalog",
        "Eingabekontrolle": "Siehe DS-Fragenkatalog",
        "Auftragskontrolle": "Siehe DS-Fragenkatalog",
        "Verfügbarkeitskontrolle": "Siehe DS-Fragenkatalog",
        "Trennungsgebot": "Siehe DS-Fragenkatalog",
        "Datenschutz-Folgenabschätzung (Art.\xa035\xa0DSGVO)": (
            "Do voluptate amet labore fugiat enim commodo ea pariatur cillum "
            "incididunt occaecat."
        ),
        "Besteht die Verarbeitung aus mehreren Dateien/Datenbanken": "nein",
        "Beschreibung der Verarbeitungstätigkeit (Dateien)": "None",
        "Auskunftsperson": "Max Mustermann",
        "Bemerkungen": "Team QS ist direkt der Abteilung zugeordnet.",
        "Aufnahme-/Änderungsdatum": "01.01.2019",
        "Erfassername": "Mustermann, Moritz",
    }
    assert (
        details.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
        == expected_details
    )


def test_parse_single_entry_minimal() -> None:
    with open(TEST_DATA_DIR / "single_entry_minimal.htm", "rb") as handle:
        details = parse_single_item_html(
            handle.read(),
            "verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        )
    expected_details = {
        "item_id": 39,
        "Bezeichnung der Verarbeitungstätigkeit": '"Qualitätssicherung Gesundheitsstudie"',
        "Version": "1.0",
        "Kurzbeschreibung": "Datenqualitätsanalysen",
    }
    assert (
        details.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
        == expected_details
    )


def test_parse_detail_block() -> None:
    soup = BeautifulSoup(
        """
        <div class="detail_block">
            <div class="input_vorgabe"><span>Bezeichnung der Verarbeitungstätigkeit:</span></div>
            <div class="input_feld">"Qualitätssicherung Gesundheitsstudie"</div>
        </div>
        """,
        "html.parser",
    )
    k, v = parse_detail_block(soup)
    assert k == "Bezeichnung der Verarbeitungstätigkeit"
    assert v == '"Qualitätssicherung Gesundheitsstudie"'


def test_parse_detail_block_raises_extractor_error_on_missing_tag() -> None:
    soup = BeautifulSoup(
        """
        <div class="detail_block">
            <div class="input_vorgabe"><span>Bezeichnung der Verarbeitungstätigkeit:</span></div>
        </div>
        """,
        "html.parser",
    )
    with pytest.raises(MExError):
        parse_detail_block(soup)
