import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from pytest import MonkeyPatch

from mex.extractors.igs.connector import IGSConnector

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture
def mocked_igs(monkeypatch: MonkeyPatch) -> None:
    """Mock the IGS connector to return dummy data."""
    monkeypatch.setattr(
        IGSConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock(spec=requests.Session)),
    )
    with (TEST_DATA_DIR / "openapi.json").open(encoding="utf-8") as fh:
        test_data = json.load(fh)
    monkeypatch.setattr(
        IGSConnector,
        "get_json_from_api",
        lambda *_, **__: test_data,
    )
