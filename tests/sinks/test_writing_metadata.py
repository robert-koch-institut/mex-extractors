import datetime
import json
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from packaging.version import InvalidVersion
from pytest import MonkeyPatch

from mex.extractors.sinks.write_metadata import (
    build_directory_path,
    calculate_checksum,
    create_metadata_content,
)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        pytest.param("1.2.3", "lorem-1.2", id="short-version"),
        pytest.param("123.456.789", "lorem-123.456", id="long-version"),
        pytest.param("1.2.7-beta", "lorem-1.2", id="version-with-letters"),
    ],
)
def test_build_directory_path(
    monkeypatch: MonkeyPatch, version: str, expected: str
) -> None:
    def fake_version(module: str) -> str:
        assert module == "mex-model", (
            f"Expected call with mex-model, was called with {module}"
        )
        return version

    monkeypatch.setattr(
        "mex.extractors.sinks.write_metadata.metadata.version", fake_version
    )

    test_path = build_directory_path("lorem")
    assert test_path.as_posix() == expected


def test_build_directory_path_exception(monkeypatch: MonkeyPatch) -> None:
    version = "bogus.version"

    def fake_version(module: str) -> str:
        assert module == "mex-model", (
            f"Expected call with mex-model, was called with {module}"
        )
        return version

    monkeypatch.setattr(
        "mex.extractors.sinks.write_metadata.metadata.version", fake_version
    )
    with pytest.raises(InvalidVersion, match=r"Invalid version: 'bogus.version'"):
        build_directory_path("lorem")


def test_calculate_checksum() -> None:
    expected = "8b7df143d91c716ecfa5fc1730022f6b421b05cedee8fd52b1fc65a96030ad52"
    with BytesIO() as buffer:
        buffer.write(b"blah")
        returned = calculate_checksum(buffer)
    assert returned == expected


@pytest.mark.usefixtures("mocked_backend_s3")
def test_create_metadata_content(monkeypatch: MonkeyPatch) -> None:
    locally_available_version = {
        "mex-backend": "mex-backend-version",
        "mex-common": "mex-common-version",
        "mex-extractors": "mex-extractors-version",
        "mex-model": "mex-model-version",
    }
    sha256_checksum = "checksum"
    write_completed_at = "2123-12-31T23:59:59.123456+00:00"
    expected_content = {
        "versions": locally_available_version,
        "sha256_checksum": sha256_checksum,
        "write_completed_at": write_completed_at,
    }

    # patch version
    def mock_version(module: str) -> str:
        assert module in locally_available_version, (
            f"Unsupported module '{module}', Supported: {locally_available_version.keys()}"
        )
        return locally_available_version[module]

    monkeypatch.setattr(
        "mex.extractors.sinks.write_metadata.metadata.version", mock_version
    )

    # patch date
    mocked_datetime = MagicMock()
    mocked_datetime.now = MagicMock(
        return_value=datetime.datetime.fromisoformat(write_completed_at)
    )
    monkeypatch.setattr(
        "mex.extractors.sinks.write_metadata.datetime.datetime", mocked_datetime
    )

    # execute
    metadata_content = create_metadata_content(sha256_checksum)
    assert json.loads(metadata_content) == expected_content
