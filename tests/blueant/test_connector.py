from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.extractors.blueant.connector import BlueAntConnector
from mex.extractors.blueant.models.project import BlueAntProject


@pytest.fixture
def mocked_blueant(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the BlueAntConnector with a MagicMock session and return that."""
    mocked_session = MagicMock(spec=requests.Session, name="mocked_blueant_session")
    mocked_session.request = MagicMock(
        return_value=Mock(spec=requests.Response, status_code=200)
    )

    mocked_session.headers = {}

    def set_mocked_session(self: BlueAntConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(BlueAntConnector, "_set_session", set_mocked_session)
    return mocked_session


def test_get_json_from_api_mocked_success(mocked_blueant: MagicMock) -> None:
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(
        return_value={"status": {"name": "OK", "code": 200}}
    )
    mocked_blueant.request = MagicMock(return_value=mocked_response)
    connector = BlueAntConnector.get()
    returned = connector._get_json_from_api("foo")
    assert returned == mocked_response.json.return_value


def test_get_json_from_api_mocked_error(mocked_blueant: MagicMock) -> None:
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(
        return_value={"status": {"name": "NOT_FOUND", "code": 404}}
    )
    mocked_blueant.request = MagicMock(return_value=mocked_response)
    connector = BlueAntConnector.get()
    with pytest.raises(MExError, match=r"NOT_FOUND"):
        connector._get_json_from_api("foo")


@pytest.mark.requires_rki_infrastructure
def test_initialization() -> None:
    connector = BlueAntConnector.get()
    assert str(connector.session.headers["Authorization"]).startswith("Bearer")


@pytest.mark.requires_rki_infrastructure
def test_get_projects() -> None:
    connector = BlueAntConnector.get()
    blueant_projects = connector.get_projects()
    assert blueant_projects[0].model_fields_set == BlueAntProject.model_fields.keys()


def test_get_projects_mocked(mocked_blueant: BlueAntConnector) -> None:
    project_dict = {
        "projects": [
            {
                "departmentId": 39866,
                "typeId": 1025,
                "statusId": 3456,
                "clients": [{"clientId": 12345, "share": 100.0}],
                "projectLeaderId": 54689,
                "name": "Inbetriebnahme der absoluten Gesundheit",
                "number": "007",
                "start": "2012-07-12",
                "end": "2013-07-11",
                "random_additional_value": "unnecessary info",
            }
        ]
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=project_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    projects = connector.get_projects()

    assert len(projects) == 1
    assert projects[0] == BlueAntProject.model_validate(project_dict["projects"][0])


@pytest.mark.requires_rki_infrastructure
def test_get_client_name() -> None:
    connector = BlueAntConnector.get()
    name = connector.get_client_name(19611103)
    assert name == "Robert Koch-Institut"


def test_get_client_name_mocked(mocked_blueant: BlueAntConnector) -> None:
    expected_name = "The Health"

    client_name_dict = {
        "status": {"name": "OK"},
        "customer": {
            "text": expected_name,
            "position": 0,
        },
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=client_name_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    name = connector.get_client_name(12345)

    assert name == expected_name


@pytest.mark.requires_rki_infrastructure
def test_get_type_description() -> None:
    connector = BlueAntConnector.get()
    description = connector.get_type_description(18426)
    assert description == "Standardprojekt"


def test_get_type_description_mocked(mocked_blueant: BlueAntConnector) -> None:
    expected_description = "Standardprojekt"

    type_description_dict = {
        "status": {"name": "OK"},
        "type": {
            "description": expected_description,
            "position": 0,
        },
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=type_description_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    name = connector.get_type_description(12345)

    assert name == expected_description


@pytest.mark.requires_rki_infrastructure
def test_get_status_name() -> None:
    connector = BlueAntConnector.get()
    description = connector.get_status_name(18438)
    assert description == "Projektumsetzung"


def test_get_status_name_mocked(mocked_blueant: BlueAntConnector) -> None:
    expected_description = "Projektumsetzung"

    status_name_dict = {
        "status": {"name": "OK"},
        "projectStatus": {
            "text": expected_description,
            "position": 0,
        },
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=status_name_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    name = connector.get_status_name(12345)

    assert name == expected_description


@pytest.mark.requires_rki_infrastructure
def test_get_department_name() -> None:
    connector = BlueAntConnector.get()
    description = connector.get_department_name(39866)
    assert "Zentrale Dienste und Fachanwendungen" in description


def test_get_department_name_mocked(mocked_blueant: BlueAntConnector) -> None:
    expected_name = "Special Health Forces"
    get_department_name_dict = {
        "status": {"name": "OK"},
        "department": {
            "text": expected_name,
            "position": 0,
        },
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=get_department_name_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    name = connector.get_department_name(12345)

    assert name == expected_name


@pytest.mark.requires_rki_infrastructure
def test_get_persons() -> None:
    connector = BlueAntConnector.get()
    persons = connector.get_persons()
    assert len(persons)


def test_get_persons_mocked(mocked_blueant: BlueAntConnector) -> None:
    expected = {
        "id": 23054,
        "personnelNumber": "12345",
        "firstname": "Max",
        "lastname": "Mustermann",
        "email": "Muster@example1.com",
    }
    persons_dict = {
        "status": {"name": "OK"},
        "persons": [
            expected
            | {
                "ignore_this": "useless info",
            }
        ],
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=persons_dict)
    mocked_blueant.request = MagicMock(return_value=mocked_response)  # type: ignore[method-assign]

    connector = BlueAntConnector.get()
    persons = connector.get_persons()
    assert len(persons) == 1
    assert persons[0].model_dump() == expected
