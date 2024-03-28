from unittest.mock import MagicMock
from uuid import UUID

import pytest
import requests
from pytest import MonkeyPatch

from mex.blueant.connector import BlueAntConnector
from mex.blueant.models.person import BlueAntPerson
from mex.blueant.models.project import BlueAntProject
from mex.blueant.models.source import BlueAntSource
from mex.blueant.settings import BlueAntSettings
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPerson
from mex.common.models import ExtractedPerson
from mex.common.types import Identifier, TemporalEntity

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


@pytest.fixture(autouse=True)
def settings() -> BlueAntSettings:
    """Load the settings for this pytest session."""
    return BlueAntSettings.get()


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson(
        departmentOrUnit=Identifier("bFQoRhcVH5DHUU"),
        email="samples@rki.de",
        familyName="Sample",
        givenName="Sam",
        wasExtractedFrom=Identifier("bFQoRhcVH5DHVy"),
        identifierInPrimarySource="sam",
        worksFor=Identifier("bFQoRhcVH5DHUY"),
        label="Sample, Sam",
    )


@pytest.fixture
def blueant_source() -> BlueAntSource:
    """Return a sample Blue Ant source."""
    return BlueAntSource(
        end=TemporalEntity(2019, 12, 31),
        name="3_Prototype Space Rocket",
        number="00123",
        start=TemporalEntity(2019, 1, 7),
        client_names="Robert Koch-Institut",
        department="C1",
        projectLeaderEmployeeId="person-567",
        status="Projektumsetzung",
        type_="Standardprojekt",
    )


@pytest.fixture
def blueant_source_without_leader() -> BlueAntSource:
    """Return a sample Blue Ant source without a project leader."""
    return BlueAntSource(
        end=TemporalEntity(2010, 10, 11),
        name="2_Prototype Moon Lander",
        number="00255",
        start=TemporalEntity(2018, 8, 9),
        client_names="Robert Koch-Institut",
        department="C1 Child Department",
        status="Projektumsetzung",
        type_="Sonderforschungsprojekt",
    )


@pytest.fixture
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    """Mock the LDAP connector to return resolved persons and units."""
    monkeypatch.setattr(
        LDAPConnector,
        "__init__",
        lambda self: setattr(self, "_connection", MagicMock()),
    )
    monkeypatch.setattr(
        LDAPConnector,
        "get_persons",
        lambda _, **__: iter(
            [
                LDAPPerson(
                    employeeID="42",
                    sn="Resolved",
                    givenName="Roland",
                    displayName="Resolved, Roland",
                    objectGUID=UUID(int=1, version=4),
                    department="PARENT-UNIT",
                )
            ]
        ),
    )


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
        "get_persons",
        lambda *_, **__: [
            BlueAntPerson(
                id=MOCKED_API_SOURCE["projectLeaderId"],
                personnelNumber="42",
                firstname="Max",
                lastname="Mustermann",
                email="max@example1.com",
            )
        ],
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
