from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.extractors.rdmo.connector import RDMOConnector


@pytest.fixture
def mocked_rdmo(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the RDMOConnector with a MagicMock session and return that."""
    mocked_session = MagicMock(spec=requests.Session, name="mocked_rdmo_session")
    mocked_session.request = MagicMock(
        return_value=Mock(spec=requests.Response, status_code=200)
    )

    mocked_session.headers = {}
    mocked_session.cookies = {"csrftoken": "dummy_token"}

    def set_mocked_session(self: RDMOConnector) -> None:
        self.session = mocked_session

    monkeypatch.setattr(RDMOConnector, "_set_session", set_mocked_session)
    return mocked_session


def test_rdmo_connector_get_sources_mocked(mocked_rdmo: requests.Session) -> None:
    dummy_sources = [{"id": 123, "title": "abc"}]
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_sources)
    mocked_rdmo.request = MagicMock(return_value=mocked_response)

    connector = RDMOConnector.get()
    sources = list(connector.get_sources())

    assert len(sources)
    assert sources[0].id == 123
    assert sources[0].title == "abc"


@pytest.mark.integration
def test_rdmo_connector_get_sources() -> None:
    connector = RDMOConnector.get()
    sources = list(connector.get_sources())
    assert len(sources)
    assert sources[0].id


def test_rdmo_connector_get_question_path_mocked(mocked_rdmo: requests.Session) -> None:
    dummy_question_path = [
        {
            "uri": "https://rdmo/terms/questions/general/some-question",
            "uri_prefix": "https://rdmo/terms",
            "extra_key": "foo",
        }
    ]
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_question_path)
    mocked_rdmo.request = MagicMock(return_value=mocked_response)

    connector = RDMOConnector.get()
    question_path = connector.get_question_path(1)

    assert question_path == "/questions/general/some-question"


def test_rdmo_connector_get_question_path_not_found_mocked(
    mocked_rdmo: requests.Session,
) -> None:
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=[])
    mocked_rdmo.request = MagicMock(return_value=mocked_response)

    with pytest.raises(MExError, match="No question found"):
        connector = RDMOConnector.get()
        connector.get_question_path(1)


@pytest.mark.integration
def test_rdmo_connector_get_question_path() -> None:
    connector = RDMOConnector.get()
    question_path = connector.get_question_path(900)

    assert question_path.startswith("/questions")


def test_rdmo_connector_get_option_key_mocked(mocked_rdmo: requests.Session) -> None:
    dummy_option_key = {
        "id": 1,
        "key": "some-option",
        "extra_key": "foo",
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_option_key)
    mocked_rdmo.request = MagicMock(return_value=mocked_response)

    connector = RDMOConnector.get()
    option_key = connector.get_option_key(1)

    assert option_key == "some-option"


@pytest.mark.skip(reason="RDMO platform is due to major changes: MX-1535")
@pytest.mark.integration
def test_rdmo_connector_get_option_key() -> None:
    connector = RDMOConnector.get()
    option_key = connector.get_option_key(1956)

    assert option_key == "mf4"


def test_rdmo_connector_get_question_answer_pairs_mocked(
    mocked_rdmo: requests.Session, monkeypatch: MonkeyPatch
) -> None:
    dummy_question_answer_pairs = [
        {
            "attribute": 0,
            "text": "has-text",
            "option": None,
            "extra_key": "foo",
        },
        {
            "attribute": 1,
            "text": None,
            "option": 42,
        },
        {
            "attribute": 2,
            "text": None,
            "option": None,
        },
    ]
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=dummy_question_answer_pairs)
    mocked_rdmo.request = MagicMock(return_value=mocked_response)

    connector = RDMOConnector.get()
    monkeypatch.setattr(
        connector,
        "get_question_path",
        lambda attr: f"/questions/{attr}/some-question",
    )
    monkeypatch.setattr(connector, "get_option_key", lambda option: f"option-{option}")

    question_answer_pairs = connector.get_question_answer_pairs(99)

    assert question_answer_pairs == {
        "/questions/0/some-question": "has-text",
        "/questions/1/some-question": "option-42",
    }


@pytest.mark.skip(reason="RDMO platform is due to major changes: MX-1535")
@pytest.mark.integration
def test_rdmo_connector_get_question_answer_pairs() -> None:
    connector = RDMOConnector.get()
    question_answer_pairs = connector.get_question_answer_pairs(34)

    assert len(question_answer_pairs) >= 10
