import json
from collections.abc import Iterable

import boto3

from mex.common.logging import logger
from mex.extractors.datenkompass.item import AnyDatenkompassModel
from mex.extractors.settings import Settings


def write_item_to_json(
    datenkompassitems: Iterable[AnyDatenkompassModel],
) -> None:
    """Write activity to json."""
    settings = Settings.get()
    session = boto3.Session(
        aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
    )

    s3 = session.client("s3", endpoint_url=str(settings.s3_endpoint_url))

    datenkompassitems = list(datenkompassitems)
    file_name = f"report-server_{datenkompassitems[0].entityType}.json"
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
