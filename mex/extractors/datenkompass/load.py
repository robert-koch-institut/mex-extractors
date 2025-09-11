import io
from collections.abc import Sequence

import boto3
import pandas as pd
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


def write_items_to_xlsx(
    datenkompassitems: Sequence[
        DatenkompassActivity | DatenkompassBibliographicResource | DatenkompassResource,
    ],
    s3: BaseClient,
) -> None:
    """Write Datenkompass items to xlsx.

    Args:
        datenkompassitems: List of Datenkompass items.
        s3: S3 session.
    """
    settings = Settings()
    delim = settings.datenkompass.list_delimiter
    file_name = f"datenkompass_{datenkompassitems[0].entityType}.xlsx"

    dicts = [
        item.model_dump(by_alias=True, exclude_none=True) for item in datenkompassitems
    ]

    df = pd.DataFrame(dicts)

    for col in df.columns:
        df[col] = df[col].apply(
            lambda v: delim.join(map(str, v)) if isinstance(v, list) else v
        )

    xlsx_buf = io.BytesIO()
    df.to_excel(xlsx_buf, index=False)
    xlsx_buf.seek(0)

    s3.put_object(
        Bucket=settings.s3_bucket_key,
        Key=file_name,
        Body=xlsx_buf.getvalue(),
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    logger.info(f"Wrote {df.shape[0]} items to file {file_name}.")

    def write_items_xlsx_to_s3_passthrough(items, s3, file_name, delim=";"):
        # Make two independent iterators from items
        items_for_write, items_for_yield = tee(items)

        dicts = [
            (it.model_dump(by_alias=True, exclude_none=True)
             if hasattr(it, "model_dump") else it)
            for it in items_for_write
        ]

        df = pd.DataFrame(dicts)
        if df.empty:
            df = pd.DataFrame({"_empty": []})
        df = df.reindex(sorted(df.columns), axis=1)

        def norm(v):
            if isinstance(v, list):
                return delim.join("" if x is None else str(x) for x in v)
            if isinstance(v, (dict, tuple, set)):
                return json.dumps(v, ensure_ascii=False, sort_keys=True)
            return v

        for col in df.columns:
            df[col] = df[col].map(norm)

        xlsx_buf = io.BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        xlsx_buf.seek(0)

        s3.put_object(
            Bucket=settings.s3_bucket_key,
            Key=file_name,
            Body=xlsx_buf.getvalue(),
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Passthrough: yield the original items
        yield from items_for_yield
