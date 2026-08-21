import csv
import hashlib
import json
import re
from collections import deque
from io import BytesIO, StringIO
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.testing import Joker
from mex.common.transform import MExEncoder
from mex.extractors.publisher.models import BibliographicResourceForCsv
from mex.extractors.sinks.s3 import S3CsvSink, S3Sink, S3XlsxSink

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


@pytest.mark.usefixtures("mocked_s3sink_client", "mocked_backend")
def test_s3csv_load() -> None:
    items = [
        BibliographicResourceForCsv(
            contributingUnit=["FG 1"],
            publicationYear="2024",
            creator=["Dr. Alice Example"],
            title=["Publication"],
            journal=["Journal"],
            doi="10.1234/example-a",
            accessRestriction="open",
            publisher=None,
        ),
    ]

    sink = S3CsvSink()
    returned_items = list(sink.load(items, unit_name="FG 1"))

    assert returned_items == items
    assert sink.client.put_object.call_count == 2

    load_items_client_call, load_metadata_client_call = (
        sink.client.put_object.call_args_list
    )

    assert load_items_client_call == call(
        Body=Joker(),
        Bucket="s3_bucket",
        Key=Joker(),
        ContentType="text/csv; charset=utf-8",
    )
    assert re.match(
        r"downloadable files-\d+\.\d+/Publications_FG1\.csv",
        load_items_client_call.kwargs["Key"],
    )

    csv_bytes = sink.client.bodies[0]
    csv_text = csv_bytes.decode("utf-8")
    rows = list(csv.DictReader(StringIO(csv_text), delimiter=";"))

    assert rows == [
        {
            "contributingUnit": "['FG 1']",
            "publicationYear": "2024",
            "creator": "['Dr. Alice Example']",
            "title": "['Publication']",
            "journal": "['Journal']",
            "doi": "10.1234/example-a",
            "accessRestriction": "open",
            "publisher": "",
        },
    ]

    expected_checksum = hashlib.sha256(csv_bytes).hexdigest()

    assert load_metadata_client_call == call(
        Body=Joker(),
        Bucket="s3_bucket",
        Key=Joker(),
    )
    assert re.match(
        r"downloadable files-\d+\.\d+/metadata_FG1\.json",
        load_metadata_client_call.kwargs["Key"],
    )

    metadata_bytes = load_metadata_client_call.kwargs["Body"]
    assert isinstance(metadata_bytes, bytes)

    metadata_dct = json.loads(metadata_bytes.decode("utf-8"))
    assert metadata_dct["sha256_checksum"] == expected_checksum
    assert set(metadata_dct) == {
        "sha256_checksum",
        "versions",
        "write_completed_at",
    }


@pytest.mark.usefixtures("mocked_s3sink_client")
def test_s3csv_load_requires_unit_name() -> None:
    item = BibliographicResourceForCsv(
        contributingUnit=["FG 1"],
        publicationYear="2024",
        creator=["Dr. Alice Example"],
        title=["Publication"],
        journal=["Journal"],
        doi="10.1234/example-a",
        accessRestriction="open",
        publisher=None,
    )

    sink = S3CsvSink()

    with pytest.raises(
        RuntimeError, match=r"No Unit Name provided for loading publications."
    ):
        list(sink.load([item]))
