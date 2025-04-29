from collections import deque
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.models import ExtractedOrganization
from mex.common.testing import Joker
from mex.extractors.sinks.s3 import S3Sink


@pytest.fixture
def mocked_boto(monkeypatch: MonkeyPatch) -> MagicMock:
    mocked_client = MagicMock()
    monkeypatch.setattr(
        S3Sink, "__init__", lambda self: setattr(self, "client", mocked_client)
    )
    return mocked_client


def test_s3_load(
    extracted_organization_rki: ExtractedOrganization, mocked_boto: MagicMock
) -> None:
    s3 = S3Sink.get()
    deque(s3.load([extracted_organization_rki]), maxlen=0)

    assert mocked_boto.put_object.call_args.kwargs == {
        "Body": Joker(),
        "Bucket": "s3_bucket",
        "Key": "publisher.ndjson",
    }
    assert mocked_boto.put_object.call_args.kwargs["Body"]
