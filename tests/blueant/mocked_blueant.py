from unittest.mock import MagicMock

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.types import TemporalEntity
from mex.extractors.blueant.connector import BlueAntConnector
from mex.extractors.blueant.models.person import BlueAntPerson
from mex.extractors.blueant.models.project import BlueAntProject

MOCKED_API_SOURCE = {
    "clients": [{"clientId": 1}],
    "departmentId": 123,
    "end": TemporalEntity(2019, 12, 31),
    "name": "Prototype Space Rocket",
    "number": "00123",
    "projectLeaderId": 12345,
    "start": TemporalEntity(2019, 1, 7),
    "statusId": 3215,
    "typeId": 8762,
}
MOCKED_RESOLVED_ATTRIBUTES = {
    "client_names": ["Robert Koch-Institut"],
    "department": "C1: Sub Unit",
    "projectLeaderEmployeeId": "42",
    "status": "Projektumsetzung",
    "type_": "Standardprojekt",
}


@pytest.fixture
def mocked_blueant(monkeypatch: MonkeyPatch) -> None:
    """Mock the blueant connector to return dummy data."""
    monkeypatch.setattr(
        BlueAntConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock(spec=requests.Session)),
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_person",
        lambda *_, **__: BlueAntPerson(
            id=MOCKED_API_SOURCE["projectLeaderId"],
            personnelNumber="42",
            firstname="Max",
            lastname="Mustermann",
            email="max@example1.com",
        ),
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_projects",
        lambda *_, **__: [BlueAntProject.model_validate(MOCKED_API_SOURCE)],
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_type_description",
        lambda *_, **__: MOCKED_RESOLVED_ATTRIBUTES["type_"],
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_status_name",
        lambda *_, **__: MOCKED_RESOLVED_ATTRIBUTES["status"],
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_department_name",
        lambda *_, **__: MOCKED_RESOLVED_ATTRIBUTES["department"],
    )
    monkeypatch.setattr(
        BlueAntConnector,
        "get_client_name",
        lambda *_, **__: MOCKED_RESOLVED_ATTRIBUTES["client_names"][0],
    )
