import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch
from requests import Session

from mex.extractors.drop import DropApiConnector


class MockedResponse:
    content: bytes


@pytest.fixture
def mocked_drop(monkeypatch: MonkeyPatch) -> None:
    """Mock the drop api connector to return dummy data."""
    monkeypatch.setattr(
        DropApiConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock(spec=Session)),
    )
    monkeypatch.setattr(
        DropApiConnector,
        "list_files",
        lambda _, x_system: [
            path.stem
            for path in (
                Path(__file__).parents[2]
                / "tests"
                / x_system.replace("-", "_")
                / "test_data"
            ).rglob("*.*")
        ],
    )

    def get_file_mocked(
        _self: DropApiConnector, x_system: str, file_id: str
    ) -> dict[str, Any]:
        path = (
            Path(__file__).parents[2]
            / "tests"
            / x_system.replace("-", "_")
            / "test_data"
            / file_id
        ).with_suffix(".json")
        with path.open() as handle:
            return json.load(handle)

    monkeypatch.setattr(
        DropApiConnector,
        "get_file",
        get_file_mocked,
    )

    def get_raw_file_mocked(
        _self: DropApiConnector, x_system: str, file_id: str
    ) -> "MockedResponse":
        with (
            (
                Path(__file__).parents[2]
                / "tests"
                / x_system.replace("-", "_")
                / "test_data"
                / file_id
            ).with_suffix(".xml")
        ).open(
            encoding="utf-8",
        ) as f:
            mocked_response = MockedResponse()
            mocked_response.content = f.read()
            return mocked_response

    monkeypatch.setattr(
        DropApiConnector,
        "get_raw_file",
        get_raw_file_mocked,
    )
