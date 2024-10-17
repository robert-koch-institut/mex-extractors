from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.datscha_web.connector import DatschaWebConnector
from mex.extractors.datscha_web.models.item import DatschaWebItem


@pytest.fixture
def mocked_datscha_web(
    monkeypatch: MonkeyPatch,
    datscha_web_item: DatschaWebItem,
    datscha_web_item_without_contributors: DatschaWebItem,
) -> None:
    """Mock the datscha web connector to return dummy data."""
    mock_items = {
        "fake://17": datscha_web_item,
        "fake://92": datscha_web_item_without_contributors,
    }
    monkeypatch.setattr(
        DatschaWebConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock()),
    )
    monkeypatch.setattr(
        DatschaWebConnector, "get_item_urls", lambda _: iter(mock_items)
    )
    monkeypatch.setattr(DatschaWebConnector, "get_item", lambda _, n: mock_items[n])


@pytest.fixture
def datscha_web_item_raw() -> dict[str, Any]:
    """Return a raw datscha item."""
    return {
        "item_id": 17,
        "Bezeichnung der Verarbeitungstätigkeit": (
            "Consequuntur atque reiciendis voluptates minus."
        ),
        "Version": "1.0",
        "Kurzbeschreibung": (
            "Est quas tempore placeat. Nam explicabo et odit dignissimos mollitia "
            "ipsam. Ea rem molestias accusamus quaerat id repudiandae. A laudantium "
            "sint doloribus eveniet sit deleniti necessitatibus."
        ),
        "Zentrale Stelle für die Verarbeitung": "Abteilung 2",
        "Zweckbestimmung der Datenverarbeitung": "wichtiges dinge",
        "Rechtsgrundlage": "Aus Gründen",
        "Herkunft der Daten": "Aus der Datenherkunft.",
        "Betroffene Personen": (
            "Die Teilnehmenden der Studie kommen aus den "
            "Studienregionen und sind bei Ziehung zwischen 18 und 69 Jahre alt."
        ),
        "Daten oder Datenkategorien": (
            "Studien-/ Gesundheitsdaten (Alter, Geschlecht, Untersuchungsvariablen)."
        ),
        "Löschfrist der Daten": (
            "Laut Vorgaben der Studie sind die Daten zwei Tage nach Erhalt zu löschen."
        ),
        "Zugriffsberechtigte": (
            "Die Mitarbeitenden der externen Qualitätssicherung in der Abteilung 2."
        ),
        "Gemeinsames/Verbundenes Verfahren": "nein",
        "Abrufverfahren": "nein",
        "Verantwortlicher für das Abrufverfahren": "None",
        "EU-Verfahren": "nein",
        "Auftragsverarbeitung (Art.\xa028\xa0DSGVO)": "nein",
        "Auftragsverarbeiter": "None",
        "Datenverarbeitung (Übermittlung/Offenlegung)": "nein",
        "Art der verarbeiteten (übermittelten/offengelegten) Daten": "None",
        "Empfänger der verarbeiteten (übermittelten/offengelegten) Daten": "None",
        "Datenübermittlung in Drittstaat": "nein",
        "Name des Drittstaats": "None",
        "Art der verarbeiteten (übermittelten/offengelegten) Daten in Drittstaat": "None",
        "Empfänger der Daten im Drittstaat": "Fancy Fake Firm & CoKG",
        "Hard- und Software": "test://dms-vbs/fsc/mx/COO.2219.100.4.251163>",
        "Schutzmaßnahmen (Art.\xa032\xa0DSGVO)": "Siehe DS-Fragenkatalog",
        "Zugriffskontrolle": "Siehe DS-Fragenkatalog",
        "Weitergabekontrolle": "Siehe DS-Fragenkatalog",
        "Eingabekontrolle": "Siehe DS-Fragenkatalog",
        "Auftragskontrolle": "Siehe DS-Fragenkatalog",
        "Verfügbarkeitskontrolle": "Siehe DS-Fragenkatalog",
        "Trennungsgebot": "Siehe DS-Fragenkatalog",
        "Datenschutz-Folgenabschätzung (Art.\xa035\xa0DSGVO)": "Keine Folgen.",
        "Besteht die Verarbeitung aus mehreren Dateien/Datenbanken": "nein",
        "Beschreibung der Verarbeitungstätigkeit (Dateien)": "None",
        "Auskunftsperson": "Coolname, Cordula/ Ausgedacht, Alwina",
        "Bemerkungen": "Das ist noch zu bemerken.",
        "Aufnahme-/Änderungsdatum": "01.01.2019",
        "Erfassername": "Doe, John",
        "Liegenschaften/Organisationseinheiten (LOZ)": [
            "L1",
            "FG99",
            "L",
            "FG666",
            "C1 Unterabteilung",
        ],
    }


@pytest.fixture
def datscha_web_item(datscha_web_item_raw: dict[str, Any]) -> DatschaWebItem:
    """Return a datscha web item instance."""
    return DatschaWebItem.model_validate(datscha_web_item_raw)


@pytest.fixture
def datscha_web_item_without_contributors(
    datscha_web_item_raw: dict[str, Any],
) -> DatschaWebItem:
    """Return datscha web item without 'auskunftsperson' and 'zentrale_stelle_fdv'."""
    return DatschaWebItem.model_validate(
        {
            **datscha_web_item_raw,
            "item_id": 92,
            "Auskunftsperson": None,
            "Zentrale Stelle für die Verarbeitung": None,
        }
    )
