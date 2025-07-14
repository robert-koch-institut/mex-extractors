import json
from collections.abc import Sequence

import boto3
from botocore.client import BaseClient

from mex.common.logging import logger
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
    DatenkompassResource,
)
from mex.extractors.settings import Settings


def start_s3_client() -> BaseClient:
    """Start up S3 session.

    Returns:
        BaseClient of a S3 session.
    """
    settings = Settings.get()
    session = boto3.Session(
        aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
    )

    return session.client("s3", endpoint_url=str(settings.s3_endpoint_url))


def write_item_to_json(
    datenkompassitems: Sequence[
        DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource
    ],
    s3: BaseClient,
) -> None:
    """Write Datenkompass items to json.

    Args:
        datenkompassitems: List of Datenkompass items.
        s3: S3 session.
    """
    settings = Settings.get()

    file_name = f"datenkompass_{datenkompassitems[0].entityType}.json"
    file_content = json.dumps(
        [item.model_dump(by_alias=True) for item in datenkompassitems],
        indent=2,
        ensure_ascii=False,
    )

    s3.put_object(
        Bucket=settings.s3_bucket_key,
        Key=file_name,
        Body=file_content.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    logger.info(
        "Written %s items to file '%s' on S3", len(datenkompassitems), file_name
    )
