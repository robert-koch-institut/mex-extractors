import json
from collections.abc import Generator, Iterable
from io import BytesIO

import boto3
import pandas as pd

from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.common.utils import flatten_pydantic_model
from mex.extractors.settings import Settings


class S3Base(BaseSink):
    """Base Sink to load models into S3 bucket."""

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

    def close(self) -> None:
        """Close the underlying boto client."""
        self.client.close()

    def load(self, items: Iterable[BaseModel]) -> Generator[BaseModel, None, None]:
        """Force subclass to implement Load method."""
        raise NotImplementedError  # force subclass to implement


class S3Sink(S3Base):
    """Standard sink to load models as NDJSON file into S3 bucket."""

    def load(self, items: Iterable[BaseModel]) -> Generator[BaseModel, None, None]:
        """Write the incoming items as an NDJSON directly to S3.

        Args:
            items: Iterable of any kind of items

        Returns:
            Generator for the loaded items
        """
        settings = Settings.get()
        total_count = 0
        with BytesIO() as buffer:
            for item in items:
                dumped_json = json.dumps(item, sort_keys=True, cls=MExEncoder)
                buffer.write(f"{dumped_json}\n".encode())
                total_count += 1
            buffer.seek(0)  # Reset buffer pointer before uploading
            self.client.put_object(
                Body=buffer,
                Bucket=settings.s3_bucket_key,
                Key="publisher.ndjson",
            )
        logger.info("%s - written %s items", type(self).__name__, total_count)
        yield from items


class S3XlsxSink(S3Base):
    """Special sink to load models as XLSX file into S3 bucket."""

    def __init__(
        self, *, separator: str, sort_columns_alphabetically: bool = True
    ) -> None:
        """Instantiate a new S3 xlsx sink with needed parameters."""
        self.separator = separator
        self.sort_columns_alphabetically = sort_columns_alphabetically

    def load(self, items: Iterable[BaseModel]) -> Generator[BaseModel, None, None]:
        """Write the incoming items as an XLSX directly to S3.

        Args:
            items: Iterable of any kind of items
            separator: Separator to use between items
            sort_columns_alphabetically: Flag if the column names should be sorted

        Returns:
            Generator for the loaded items
        """
        settings = Settings.get()
        file_name = f"{items[0].entityType}.xlsx"

        flat_dicts = [flatten_pydantic_model(item, self.separator) for item in items]

        df = pd.DataFrame(flat_dicts)
        if self.sort_columns_alphabetically:
            df = df.reindex(sorted(df.columns), axis=1)

        xlsx_buffer = BytesIO()
        with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
            df.to_excel(
                writer,
                sheet_name="Tabelle1",
                index=False,
            )
        xlsx_buffer.seek(0)

        self.client.put_object(
            Bucket=settings.s3_bucket_key,
            Key=file_name,
            Body=xlsx_buffer.getvalue(),
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        logger.info(f"Wrote {len(df)} items to {file_name}")
        yield from items
