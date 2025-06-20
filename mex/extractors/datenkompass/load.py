import json

import boto3

from mex.extractors.datenkompass.item import AnyDatenkompassModel
from mex.extractors.settings import Settings


def write_activity_to_json(
    datenkompassactivity: list[AnyDatenkompassModel],
) -> None:
    """Write activity to json."""
    settings = Settings.get()
    session = boto3.Session(
        aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
    )

    s3 = session.client("s3", endpoint_url=str(settings.s3_endpoint_url))

    file_name = f"report-server_{datenkompassactivity[0].entityType}.json"
    file_content = json.dumps(
        [item.model_dump(by_alias=True) for item in datenkompassactivity],
        indent=2,
        ensure_ascii=False,
    )

    s3.put_object(
        Bucket=settings.s3_bucket_key,
        Key=file_name,
        Body=file_content.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
