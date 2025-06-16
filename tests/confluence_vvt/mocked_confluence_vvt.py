from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.confluence_vvt import main
from mex.extractors.confluence_vvt.models import (
    ConfluenceVvtHeading,
    ConfluenceVvtPage,
    ConfluenceVvtRow,
    ConfluenceVvtTable,
    ConfluenceVvtValue,
)


@pytest.fixture
def mocked_confluence_vvt(
    monkeypatch: MonkeyPatch,
    confluence_vvt_sources_dict: dict[str, ConfluenceVvtPage],
) -> None:
    """Mock the Confluence-vvt extractor to return mocked data."""
    monkeypatch.setattr(
        main,
        "fetch_all_vvt_pages_ids",
        MagicMock(return_value=list(confluence_vvt_sources_dict)),
    )
    monkeypatch.setattr(
        main,
        "get_page_data_by_id",
        MagicMock(return_value=confluence_vvt_sources_dict.values()),
    )


@pytest.fixture
def confluence_vvt_sources_dict() -> dict[str, ConfluenceVvtPage]:
    return {
        "fake_1": ConfluenceVvtPage(
            id=1234567,
            title="First fake confluence page data.",
            tables=[
                ConfluenceVvtTable(
                    rows=[
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Interne Vorgangsnummer (Datenschutz)"
                                ),
                                ConfluenceVvtHeading(
                                    text="Zuständige/r DatenschutzkoordinatorIn für die Abteilung(Achtung: entspricht nicht der/dem DSB!)"
                                ),
                                ConfluenceVvtHeading(
                                    text="(Funktionsbezeichnung der/des zuständigen DatenschutzkoordinatorIn hier einfügen)"
                                ),
                                ConfluenceVvtHeading(
                                    text="War eine Datenschutzfolgenabschätzung notwendig? ja/nein"
                                ),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=["DS-2023-123"]),
                                ConfluenceVvtValue(texts=["M, Mustermann"]),
                                ConfluenceVvtValue(texts=[]),
                                ConfluenceVvtValue(texts=[]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Ausführliche Beschreibung des Verfahrens / der Verarbeitung / der Studie"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Absolutely uselsess text. Absolutely uselsess text. Absolutely uselsess text. Absolutely uselsess text. ",
                                    ]
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Verantwortliche(r) StudienleiterIn"
                                ),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text="Abt."),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=["Musterman, Max", "max"]),
                                ConfluenceVvtValue(texts=["FG99"]),
                                ConfluenceVvtValue(texts=["C1"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Vertreter der / des Verantwortlichen"
                                ),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text=None),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=["Al-Awlaqi, Sameh,", "Al-AwlaqiS@rki.de"]
                                ),
                                ConfluenceVvtValue(texts=["C1"]),
                                ConfluenceVvtValue(texts=["FG99"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(text="Mitarbeitende"),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text=None),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Max Mustermann",
                                        "Frida Fictitious",
                                    ]
                                ),
                                ConfluenceVvtValue(
                                    texts=[
                                        "FG99",
                                        "C1",
                                    ]
                                ),
                                ConfluenceVvtValue(texts=["FG99", "C1"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. gemeinsam Verantwortliche(r) nach Art. 26 DSGVO (Nennung der Behörde/des Unternehmens inkl. vollständige Adresse UND der Kontaktdaten des dortigen Studienleiters / der dortigen Studienleiterin)"
                                ),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=[]),
                                ConfluenceVvtValue(texts=[]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Name des /der DSB des gemeinsam Verantwortlichen nach Art. 26 DSGVO (inkl. der Kontaktdaten)"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Another absolutely useless string.",
                                    ]
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Auftragsverarbeiter nach Art. 28 DSGVO"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(cells=[ConfluenceVvtValue(texts=[])]),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Name des /der DSB des Auftragsverarbeiters nach Art. 28 DSGVO (inkl. der Kontaktdaten)"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(cells=[ConfluenceVvtValue(texts=[])]),
                    ]
                ),
                ConfluenceVvtTable(
                    rows=[
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(text="Zwecke des Vorhabens"),
                                ConfluenceVvtHeading(
                                    text="Erläuterungen (Ausfüllhinweise)"
                                ),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Oh look, another absolutely useless string.",
                                    ]
                                ),
                                ConfluenceVvtValue(
                                    texts=[
                                        "This fake data is full of useless strings, strange!",
                                    ]
                                ),
                            ]
                        ),
                    ]
                ),
            ],
        ),
        "fake_2": ConfluenceVvtPage(
            id=9876541,
            title="Second fake confluence page data.",
            tables=[
                ConfluenceVvtTable(
                    rows=[
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Interne Vorgangsnummer (Datenschutz)"
                                ),
                                ConfluenceVvtHeading(
                                    text="Zuständige/r DatenschutzkoordinatorIn für die Abteilung(Achtung: entspricht nicht der/dem DSB!)"
                                ),
                                ConfluenceVvtHeading(
                                    text="(Funktionsbezeichnung der/des zuständigen DatenschutzkoordinatorIn hier einfügen)"
                                ),
                                ConfluenceVvtHeading(
                                    text="War eine Datenschutzfolgenabschätzung notwendig? ja/nein"
                                ),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=["DS-0000-123"]),
                                ConfluenceVvtValue(texts=["M, Mustermann"]),
                                ConfluenceVvtValue(texts=[]),
                                ConfluenceVvtValue(texts=[]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Ausführliche Beschreibung des Verfahrens / der Verarbeitung / der Studie"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Absolutely uselsess text. Absolutely uselsess text. Absolutely uselsess text. Absolutely uselsess text. ",
                                    ]
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="Verantwortliche(r) StudienleiterIn"
                                ),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text="Abt."),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=["Musterman, Max", "max"]),
                                ConfluenceVvtValue(texts=["FG99"]),
                                ConfluenceVvtValue(texts=["C1"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Vertreter der / des Verantwortlichen"
                                ),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text=None),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=["Al-Awlaqi, Sameh,", "Al-AwlaqiS@rki.de"]
                                ),
                                ConfluenceVvtValue(texts=["C1"]),
                                ConfluenceVvtValue(texts=["FG99"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(text="Mitarbeitende"),
                                ConfluenceVvtHeading(text="OE"),
                                ConfluenceVvtHeading(text=None),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Max Mustermann",
                                        "Frida Fictitious",
                                    ]
                                ),
                                ConfluenceVvtValue(
                                    texts=[
                                        "FG99",
                                        "C1",
                                    ]
                                ),
                                ConfluenceVvtValue(texts=["FG99", "C1"]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. gemeinsam Verantwortliche(r) nach Art. 26 DSGVO (Nennung der Behörde/des Unternehmens inkl. vollständige Adresse UND der Kontaktdaten des dortigen Studienleiters / der dortigen Studienleiterin)"
                                ),
                                ConfluenceVvtHeading(text="Tel."),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(texts=[]),
                                ConfluenceVvtValue(texts=[]),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Name des /der DSB des gemeinsam Verantwortlichen nach Art. 26 DSGVO (inkl. der Kontaktdaten)"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Another absolutely useless string.",
                                    ]
                                )
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Auftragsverarbeiter nach Art. 28 DSGVO"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(cells=[ConfluenceVvtValue(texts=[])]),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(
                                    text="ggfs. Name des /der DSB des Auftragsverarbeiters nach Art. 28 DSGVO (inkl. der Kontaktdaten)"
                                )
                            ]
                        ),
                        ConfluenceVvtRow(cells=[ConfluenceVvtValue(texts=[])]),
                    ]
                ),
                ConfluenceVvtTable(
                    rows=[
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtHeading(text="Zwecke des Vorhabens"),
                                ConfluenceVvtHeading(
                                    text="Erläuterungen (Ausfüllhinweise)"
                                ),
                            ]
                        ),
                        ConfluenceVvtRow(
                            cells=[
                                ConfluenceVvtValue(
                                    texts=[
                                        "Oh look, another absolutely useless string.",
                                    ]
                                ),
                                ConfluenceVvtValue(
                                    texts=[
                                        "This fake data is full of useless strings, strange!",
                                    ]
                                ),
                            ]
                        ),
                    ]
                ),
            ],
        ),
    }
