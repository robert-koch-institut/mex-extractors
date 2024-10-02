from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.rdmo.connector import RDMOConnector
from mex.extractors.rdmo.models.person import RDMOPerson
from mex.extractors.rdmo.models.source import RDMOSource


@pytest.fixture
def mocked_rdmo(monkeypatch: MonkeyPatch) -> None:
    """Mock the RDMO connector to return dummy data."""
    sources = (
        RDMOSource(id=1, title="Unowned"),
        RDMOSource(
            id=2,
            title="Owned",
            owners=[
                RDMOPerson(
                    email="musterm@rki.de",
                    first_name="Max",
                    id=100,
                    last_name="Muster",
                    username="musterm",
                ),
                RDMOPerson(
                    email="beispielb@rki.de",
                    first_name="Beate",
                    id=200,
                    last_name="Beispiel",
                    username="beispielb",
                ),
            ],
        ),
    )
    monkeypatch.setattr(RDMOConnector, "get_sources", lambda _: iter(sources))

    def get_question_answer_pairs(self: RDMOConnector, project: int) -> dict[str, str]:
        return (
            {
                "/questions/project_proposal/general/project-title/title": "Lorem Ipsum",
                "/questions/project_proposal/general/project-title/acronym": "LI1",
                "/questions/project_proposal/general/project-title/project_type": "rki-internal_Project",
                "questions/project_proposal/general/project-schedule-schedule/project_start": "2004",
                "questions/project_proposal/general/project-schedule-schedule/project_end": "2006-05",
            }
            if project == 2
            else {}
        )

    monkeypatch.setattr(
        RDMOConnector, "get_question_answer_pairs", get_question_answer_pairs
    )

    def __init__(self: RDMOConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-rdmo"

    monkeypatch.setattr(RDMOConnector, "__init__", __init__)
