import datetime
import hashlib
import json
import re
from collections import deque
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, call

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import MExError
from mex.common.models import ExtractedOrganization
from mex.common.testing import Joker
from mex.common.transform import MExEncoder
from mex.extractors.sinks.s3 import S3BaseSink, S3Sink, S3XlsxSink


class MockedBoto:
    def __init__(self) -> None:
        self.put_object: MagicMock = MagicMock(side_effect=self._put_object)
        self.bodies: list[bytes] = []

    def _put_object(self, *_: Any, **kwargs: Any) -> None:  # noqa: ANN401
        body: Any = kwargs.get("Body")

        if isinstance(body, BytesIO):
            self.bodies.append(body.read())
        elif isinstance(body, bytes):
            self.bodies.append(body)
        else:
            msg = f"Unexpected Body type: {type(body)}"
            raise TypeError(msg)

    def close(self) -> None:
        pass


@pytest.fixture
def mocked_s3_client(monkeypatch: MonkeyPatch) -> MockedBoto:
    mocked_client = MockedBoto()

    def mocked_init(self: S3BaseSink) -> None:
        self.client = mocked_client

    monkeypatch.setattr(S3BaseSink, "__init__", mocked_init)
    return mocked_client


@pytest.fixture
def mocked_backend(monkeypatch: MonkeyPatch) -> BackendApiConnector:
    monkeypatch.setattr(BackendApiConnector, "_check_availability", MagicMock())
    monkeypatch.setattr(
        BackendApiConnector,
        "request",
        MagicMock(
            return_value={"status": "Fabulous", "version": "mex-backend-version"}
        ),
    )
    return BackendApiConnector.get()


@pytest.mark.usefixtures("mocked_s3_client", "mocked_backend")
def test_s3_load(extracted_organization_rki: ExtractedOrganization) -> None:
    items = [extracted_organization_rki]
    expected_str = ""
    for item in items:
        expected_str += json.dumps(item, sort_keys=True, cls=MExEncoder)
        expected_str += "\n"

    sink = S3Sink.get()
    deque(sink.load(items), maxlen=0)

    assert sink.client.put_object.call_count == 2
    load_items_client_call, load_metadata_client_call = (
        sink.client.put_object.call_args_list
    )

    assert load_items_client_call == call(
        Body=Joker(),
        Bucket="s3_bucket",
        Key=Joker(),
    )
    item_buffer = load_items_client_call.kwargs["Body"]
    assert isinstance(item_buffer, BytesIO)
    item_bytes = sink.client.bodies[0]
    item_str = item_bytes.decode("utf-8")
    assert item_str == expected_str
    assert re.match(
        r"publisher-\d+\.\d+/items.ndjson", load_items_client_call.kwargs["Key"]
    )

    expected_checksum = hashlib.sha256(item_bytes).hexdigest()

    assert load_items_client_call == call(
        Body=Joker(),
        Bucket="s3_bucket",
        Key=Joker(),
    )
    metadata_bytes = load_metadata_client_call.kwargs["Body"]
    assert isinstance(metadata_bytes, bytes)
    metadata_dct = json.loads(metadata_bytes.decode("utf-8"))
    assert metadata_dct["sha256_checksum"] == expected_checksum
    assert re.match(
        r"publisher-\d+\.\d+/metadata.json", load_metadata_client_call.kwargs["Key"]
    )


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        pytest.param("1.2.3", "publisher-1.2", id="short-version"),
        pytest.param("123.456.789", "publisher-123.456", id="long-version"),
        pytest.param("1.2.7-beta", "publisher-1.2", id="version-with-letters"),
    ],
)
def test__build_directory_path(
    monkeypatch: MonkeyPatch, version: str, expected: str
) -> None:
    def fake_version(module: str) -> str:
        assert module == "mex-model", (
            f"Expected call with mex-model, was called with {module}"
        )
        return version

    monkeypatch.setattr("mex.extractors.sinks.s3.metadata.version", fake_version)
    returned = S3Sink._build_directory_path()
    assert returned.as_posix() == expected


def test__build_directory_path_exception(monkeypatch: MonkeyPatch) -> None:
    version = "bogus.version"

    def fake_version(module: str) -> str:
        assert module == "mex-model", (
            f"Expected call with mex-model, was called with {module}"
        )
        return version

    monkeypatch.setattr("mex.extractors.sinks.s3.metadata.version", fake_version)
    with pytest.raises(
        MExError, match=r".*Cannot parse mex-model version 'bogus.version' with regex.*"
    ):
        S3Sink._build_directory_path()


def test__calculate_checksum() -> None:
    expected = "8b7df143d91c716ecfa5fc1730022f6b421b05cedee8fd52b1fc65a96030ad52"
    with BytesIO() as buffer:
        buffer.write(b"blah")
        returned = S3Sink._calculate_checksum(buffer)
    assert returned == expected


@pytest.mark.usefixtures("mocked_s3_client", "mocked_backend")
def test__load_metadata(monkeypatch: MonkeyPatch) -> None:
    locally_available_version = {
        "mex-common": "mex-common-version",
        "mex-extractors": "mex-extractors-version",
        "mex-model": "mex-model-version",
    }
    sha256_checksum = "checksum"
    write_completed_at = "2123-12-31T23:59:59.123456+00:00"
    expected_content = {
        "versions": {"mex-backend": "mex-backend-version", **locally_available_version},
        "sha256_checksum": sha256_checksum,
        "write_completed_at": write_completed_at,
    }

    # patch version
    def mock_version(module: str) -> str:
        assert module in locally_available_version, (
            f"Unsupported module '{module}', Supported: {locally_available_version.keys()}"
        )
        return locally_available_version[module]

    monkeypatch.setattr("mex.extractors.sinks.s3.metadata.version", mock_version)

    # patch date
    mocked_datetime = MagicMock()
    mocked_datetime.now = MagicMock(
        return_value=datetime.datetime.fromisoformat(write_completed_at)
    )
    monkeypatch.setattr("mex.extractors.sinks.s3.datetime.datetime", mocked_datetime)

    # execute
    sink = S3Sink.get()
    sink._load_metadata("metadata-path.json", sha256_checksum)
    assert sink.client.put_object.call_args.kwargs == {
        "Body": Joker(),
        "Bucket": "s3_bucket",
        "Key": "metadata-path.json",
    }
    body = sink.client.put_object.call_args.kwargs["Body"]
    assert isinstance(body, bytes)
    returned_content = sink.client.bodies[0].decode("utf-8")
    assert json.loads(returned_content) == expected_content


@pytest.mark.usefixtures("mocked_s3_client")
def test_s3xlsx_load(extracted_organization_rki: ExtractedOrganization) -> None:
    sink = S3XlsxSink()
    deque(sink.load([extracted_organization_rki]), maxlen=0)

    assert sink.client.put_object.call_args.kwargs == {
        "Body": Joker(),
        "Bucket": "s3_bucket",
        "Key": "ExtractedOrganization.xlsx",
        "ContentType": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    }
    assert sink.client.put_object.call_args.kwargs["Body"]
