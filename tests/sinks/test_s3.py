from unittest.mock import MagicMock

from mex.common.models import ExtractedOrganization
from mex.common.testing import Joker
from mex.extractors.sinks.s3 import S3Sink


def test_s3_load(
    extracted_organization_rki: ExtractedOrganization,
    mocked_boto: MagicMock,
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
