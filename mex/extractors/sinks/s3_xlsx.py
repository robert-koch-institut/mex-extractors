import json
from collections.abc import Generator, Iterable, Iterator
from io import BytesIO
from typing import TypeVar, Any

import boto3
import pandas as pd

from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.common.utils import grouper
from mex.extractors.settings import Settings

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class S3SinkXlsx(BaseSink):
    """Sink to load models as new-line delimited JSON file into S3 bucket."""

    CHUNK_SIZE = 1000
    SERVICE_NAME = "s3"

    def __init__(self) -> None:
        """Instantiate a new S3 sink."""
        settings = Settings.get()
        self.client = boto3.client(
            service_name=self.SERVICE_NAME,
            endpoint_url=str(settings.s3_endpoint_url),
            aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
            aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
        )
        if not settings.list_delimiter:
            self.separator = ";"

    def close(self) -> None:
        """Close the underlying boto client."""
        self.client.close()

    def grouper_stripped(
        chunk_size: int, iterable: Iterable[_LoadItemT]
    ) -> Iterator[list[_LoadItemT]]:
        """Wrapper around grouper() to remove None fill values from last chunk."""
        for group in grouper(chunk_size, iterable):
            yield [x for x in group if x is not None]

    def flatten_nested_structures(self, v: Any):
        if isinstance(v, list):
            return self.separator.join("" if x is None else str(x) for x in v)
        if isinstance(v, (dict, tuple, set)):
            return json.dumps(v, ensure_ascii=False, sort_keys=True)
        return v

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT, None, None]:
        """Write the incoming items in chunks as an XLSX directly to S3.

        Args:
            items: Iterable of any kind of items

        Returns:
            Generator for the loaded items
        """
        settings = Settings.get()
        xlsx_buffer = BytesIO()
        rows_written = 0
        first = True
        columns = None
        file_name = f"{items[0].entityType}.xlsx"

        with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
            for chunk in self.grouper_stripped(items, self.CHUNK_SIZE):
                # build DataFrame for this chunk
                dicts = [
                    item.model_dump(by_alias=True, exclude_none=True)
                    for item in chunk
                ]
                df = pd.DataFrame(dicts)

                if first:
                    columns = sorted(df.columns)
                df = df.reindex(columns=columns)

                for c in df.columns:
                    df[c] = df[c].map(self.flatten_nested_structures)

                df.to_excel(
                    writer,
                    sheet_name="Tabelle1",
                    index=False,
                    header=first,
                    startrow=0 if first else rows_written + 1,  # +1 to skip header row
                )
                rows_written += len(df)
                first = False

                for item in chunk:
                    yield item

        xlsx_buffer.seek(0)
        self.client.put_object(
            Bucket=settings.s3_bucket_key,
            Key=file_name,
            Body=xlsx_buffer.getvalue(),
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        logger.info(f"Wrote {rows_written} items to {file_name}")
