from collections.abc import Sequence

import boto3
import pandas as pd
from botocore.client import BaseClient

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


def write_items_to_csv(
    datenkompassitems: Sequence[
        DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource,
    ],
    s3: BaseClient,
) -> None:
    """Write Datenkompass items to csv.

    Args:
        datenkompassitems: List of Datenkompass items.
        s3: S3 session.
    """
    settings = Settings.get()
    list_delimiter: str = ";"
    field_delimiter: str = ","
    file_name = f"datenkompass_{datenkompassitems[0].entityType}.csv"

    dicts = [
        item.model_dump(by_alias=True, exclude_none=True) for item in datenkompassitems
    ]

    df = pd.DataFrame(dicts)

    for col in df.columns:
        df[col] = df[col].apply(
            lambda v: list_delimiter.join(map(str, v)) if isinstance(v, list) else v
        )

    csv_str = df.to_csv(index=False, sep=field_delimiter, encoding="utf-8-sig")

    s3.put_object(
        Bucket=settings.s3_bucket_key,
        Key=file_name,
        Body=csv_str.encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8",
    )
