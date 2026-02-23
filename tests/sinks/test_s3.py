import json
from collections import deque
from io import BytesIO
from typing import Any
from unittest.mock import Mock

import pytest
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.common.models import ExtractedOrganization
from mex.common.testing import Joker
from mex.common.transform import MExEncoder
from mex.extractors.sinks.s3 import S3BaseSink, S3Sink, S3XlsxSink


class MockedBoto:
    def __init__(self) -> None:
        self.put_object: Mock = Mock(side_effect=self._put_object)
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


@pytest.mark.usefixtures("mocked_s3_client")
def test_s3_load(extracted_organization_rki: ExtractedOrganization) -> None:
    items = [extracted_organization_rki]
    expected_content = ""
    for item in items:
        expected_content += json.dumps(item, sort_keys=True, cls=MExEncoder)
        expected_content += "\n"

    sink = S3Sink.get()
    deque(sink.load(items), maxlen=0)

    assert sink.client.put_object.call_args.kwargs == {
        "Body": Joker(),
        "Bucket": "s3_bucket",
        "Key": Joker(),
    }
    body = sink.client.put_object.call_args.kwargs["Body"]
    assert isinstance(body, BytesIO)
    returned_content = sink.client.bodies[0].decode("utf-8")
    assert returned_content == expected_content


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
