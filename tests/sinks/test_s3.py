import hashlib
import json
import re
from collections import deque
from io import BytesIO
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.testing import Joker
from mex.common.transform import MExEncoder
from mex.extractors.sinks.s3 import S3Sink, S3XlsxSink

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganization


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


@pytest.mark.usefixtures("mocked_s3sink_client", "mocked_backend")
def test_s3_load(extracted_organization_rki: ExtractedOrganization) -> None:
    items_generator = (item for item in [extracted_organization_rki])
    expected_items = [extracted_organization_rki]
    expected_str = ""
    for item in expected_items:
        expected_str += json.dumps(item, sort_keys=True, cls=MExEncoder)
        expected_str += "\n"

    sink = S3Sink.get()
    returned_items = list(sink.load(items_generator))

    assert returned_items == expected_items

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


@pytest.mark.usefixtures("mocked_s3sink_client")
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
