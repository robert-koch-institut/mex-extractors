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
    with (TEST_DATA_DIR / "overview.htm").open() as handle:
        urls = parse_item_urls_from_overview_html(
            handle.read(),
            "https://datscha/",
        )
    assert urls == [
        "https://datscha/verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        "https://datscha/verzeichnis_detail.php?vavs_id=46&option=Anzeigen",
    ]


def test_parse_single_item_html() -> None:
    with (TEST_DATA_DIR / "single_entry.htm").open() as handle:
        details = parse_single_item_html(
            handle.read(),
            "verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        )
    expected_details = {
        "item_id": 39,
        "Bezeichnung der Verarbeitungstätigkeit": '"Lorem Ipsum"',
        "Version": '"Lorem Ipsum"',
        "Kurzbeschreibung": '"Lorem Ipsum"',
        "Zentrale Stelle für die Verarbeitung": '"Lorem Ipsum"',
        "Zweckbestimmung der Datenverarbeitung": '"Lorem Ipsum"',
        "Rechtsgrundlage": '"Lorem Ipsum"',
        "Herkunft der Daten": '"Lorem Ipsum"',
        "Betroffene Personen": '"Lorem Ipsum"',
        "Daten oder Datenkategorien": '"Lorem Ipsum"',
        "Löschfrist der Daten": '"Lorem Ipsum"',
        "Zugriffsberechtigte": '"Lorem Ipsum"',
        "Gemeinsames/Verbundenes Verfahren": '"Lorem Ipsum"',
        "Abrufverfahren": '"Lorem Ipsum"',
        "Verantwortlicher für das Abrufverfahren": '"Lorem Ipsum"',
        "EU-Verfahren": '"Lorem Ipsum"',
        "Auftragsverarbeitung (Art.\xa028\xa0DSGVO)": '"Lorem Ipsum"',
        "Auftragsverarbeiter": '"Lorem Ipsum"',
        "Datenverarbeitung (Übermittlung/Offenlegung)": '"Lorem Ipsum"',
        "Art der verarbeiteten (übermittelten/offengelegten) Daten": '"Lorem Ipsum"',
        "Datenübermittlung in Drittstaat": '"Lorem Ipsum"',
        "Name des Drittstaats": '"Lorem Ipsum"',
        "Empfänger der Daten im Drittstaat": '"Lorem Ipsum"',
        "Hard- und Software": '"Lorem Ipsum"',
        "Schutzmaßnahmen (Art.\xa032\xa0DSGVO)": '"Lorem Ipsum"',
        "Zugriffskontrolle": '"Lorem Ipsum"',
        "Weitergabekontrolle": '"Lorem Ipsum"',
        "Eingabekontrolle": '"Lorem Ipsum"',
        "Auftragskontrolle": '"Lorem Ipsum"',
        "Verfügbarkeitskontrolle": '"Lorem Ipsum"',
        "Trennungsgebot": '"Lorem Ipsum"',
        "Datenschutz-Folgenabschätzung (Art.\xa035\xa0DSGVO)": '"Lorem Ipsum"',
        "Besteht die Verarbeitung aus mehreren Dateien/Datenbanken": '"Lorem Ipsum"',
        "Beschreibung der Verarbeitungstätigkeit (Dateien)": '"Lorem Ipsum"',
        "Auskunftsperson": '"Lorem Ipsum"',
        "Bemerkungen": '"Lorem Ipsum"',
        "Aufnahme-/Änderungsdatum": '"Lorem Ipsum"',
        "Erfassername": '"Lorem Ipsum"',
    }
    assert (
        details.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
        == expected_details
    )


def test_parse_single_entry_minimal() -> None:
    with (TEST_DATA_DIR / "single_entry_minimal.htm").open() as handle:
        details = parse_single_item_html(
            handle.read(),
            "verzeichnis_detail.php?vavs_id=39&option=Anzeigen",
        )
    expected_details = {
        "item_id": 39,
        "Bezeichnung der Verarbeitungstätigkeit": '"Lorem Ipsum"',
        "Version": '"Lorem Ipsum"',
        "Kurzbeschreibung": '"Lorem Ipsum"',
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
