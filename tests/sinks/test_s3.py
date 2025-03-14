from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.models import ExtractedOrganization
from mex.common.testing import Joker
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3Sink


@pytest.fixture
def mocked_boto(monkeypatch: MonkeyPatch) -> MagicMock:
    mocked_client = MagicMock()
    monkeypatch.setattr(
        S3Sink, "__init__", lambda self: setattr(self, "client", mocked_client)
    )
    return mocked_client


def test_s3_load(
    extracted_organization_rki: ExtractedOrganization,
    mocked_boto: MagicMock,
    settings: Settings,
) -> None:
    s3 = S3Sink.get()
    items = list(s3.load([extracted_organization_rki]))

    assert items == [extracted_organization_rki]
    assert mocked_boto.put_object.call_args.kwargs == {
        "Body": Joker(),
        "Bucket": "s3_bucket",
        "Key": "publisher.ndjson",
    }
    assert mocked_boto.put_object.call_args.kwargs["Body"].closed
    with open(settings.work_dir / "publisher.ndjson", encoding="utf-8") as fh:
        assert (
            fh.read()
            == '{"alternativeName": [], "entityType": "ExtractedOrganization", "geprisId": [], "gndId": [], "hadPrimarySource": "bFQoRhcVH5DHWp", "identifier": "hdLjlIxj8kAtpqnpZO0nmw", "identifierInPrimarySource": "Robert Koch-Institut", "isniId": [], "officialName": [{"language": "de", "value": "Robert Koch-Institut"}], "rorId": [], "shortName": [], "stableTargetId": "fxIeF3TWocUZoMGmBftJ6x", "viafId": [], "wikidataId": []}\n'
        )
