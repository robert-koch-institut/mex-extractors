import json
from io import BytesIO, StringIO
from typing import TYPE_CHECKING, TypeVar

import boto3
import pandas as pd

from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.write_metadata import (
    build_directory_path,
    calculate_checksum,
    create_metadata_content,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class S3BaseSink(BaseSink):
    """Base Sink to load models into S3 bucket."""

    SERVICE_NAME = "s3"

    def __init__(self) -> None:
        """Instantiate a new S3 sink."""
        settings = ExtractorsSettings.get()
        self.client = boto3.client(
            service_name=self.SERVICE_NAME,
            endpoint_url=str(settings.s3_endpoint_url),
            aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
            aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
            verify=settings.s3_ssl_verify,
        )

    def close(self) -> None:
        """Close the underlying boto client."""
        self.client.close()

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT]:
        """Force subclass to implement Load method."""
        raise NotImplementedError  # force subclass to implement


class S3Sink(S3BaseSink):
    """Standard sink to load models as NDJSON file into S3 bucket."""

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT]:
        """Write items.ndjson and metadata.json to S3.

        Write to directory
        `publisher-{mex-model major version}.{mex-model minor version}`
        - items to `items.ndjson`
        - metadata to `metadata.json`

        Settings:
            s3_bucket_key: The S3 Bucket key for writing to

        Args:
            items: Iterable of any kind of items

        Returns:
            Generator for the loaded items
        """
        settings = ExtractorsSettings.get()
        directory_path = build_directory_path("publisher")
        items_path = (directory_path / "items.ndjson").as_posix()
        total_count = 0
        with BytesIO() as buffer:
            for item in items:
                item_str = self._convert_item_to_ndjson(item)
                buffer.write(item_str.encode("utf-8"))
                total_count += 1
                yield item
            checksum = calculate_checksum(buffer)
            # Reset buffer pointer before uploading
            buffer.seek(0)
            self.client.put_object(
                Body=buffer,
                Bucket=settings.s3_bucket_key,
                Key=items_path,
            )
        logger.info("%s - written %s items", type(self).__name__, total_count)

        metadata_path = (directory_path / "metadata.json").as_posix()
        metadata_content = create_metadata_content(checksum)
        self.client.put_object(
            Body=metadata_content,
            Bucket=settings.s3_bucket_key,
            Key=metadata_path,
        )

        logger.info("%s - written metadata.json", type(self).__name__)

    @staticmethod
    def _convert_item_to_ndjson(item: _LoadItemT) -> str:
        """Convert an item to an ndjson string."""
        dumped_json = json.dumps(item, sort_keys=True, cls=MExEncoder)
        return f"{dumped_json}\n"


class S3XlsxSink(S3BaseSink):
    """Special sink to load models as XLSX file into S3 bucket."""

    def load(
        self,
        items: Iterable[_LoadItemT],
        *,
        primary_source_name: str | None = None,
        unit_name: str | None = None,
    ) -> Generator[_LoadItemT]:
        """Write the incoming items as an XLSX directly to S3.

        Args:
            items: Iterable of any kind of items
            primary_source_name: [optional] primary source name for excel naming
            unit_name: [optional] unit name for excel naming

        Returns:
            Generator for the loaded items
        """
        settings = ExtractorsSettings.get()
        items_list = list(items)

        optional_unit_name_extension = (
            f"_{unit_name.replace(' ', '')}" if unit_name else ""
        )
        optional_primary_source_name_extension = (
            f"_{primary_source_name}" if primary_source_name else ""
        )
        file_name = (
            f"{items_list[0].__class__.__name__}"
            f"{optional_primary_source_name_extension}"
            f"{optional_unit_name_extension}"
            f".xlsx"
        )

        dicts = [
            item.model_dump(by_alias=True, exclude_none=False) for item in items_list
        ]
        df = pd.DataFrame(dicts)

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
        yield from items_list


class S3CsvSink(S3BaseSink):
    """Special sink to load models as CDV file into S3 bucket and publish metadata."""

    def load(
        self,
        items_sorted_by_year: Iterable[_LoadItemT],
        *,
        unit_name: str | None = None,
    ) -> Generator[_LoadItemT]:
        """Write the incoming items as an CSV directly to S3.

        Args:
            items_sorted_by_year: Iterable of any kind of items
            unit_name: unit name for csv naming

        Returns:
            Generator for the loaded items
        """
        settings = ExtractorsSettings.get()

        if unit_name is None:
            msg = "No Unit Name provided."
            raise RuntimeError(msg)

        unitname = unit_name.replace(" ", "")
        directory_path = build_directory_path("downloadable files")

        publications_file_name = f"Publications_{unitname}.csv"
        publications_path = (directory_path / publications_file_name).as_posix()

        df = pd.DataFrame(
            item.model_dump(
                by_alias=True,
                exclude_none=False,
                mode="json",
            )
            for item in items_sorted_by_year
        )

        csv_buffer = StringIO(newline="")

        df.to_csv(
            csv_buffer,
            index=False,
            sep=";",
        )
        checksum = calculate_checksum(csv_buffer)

        csv_buffer.seek(0)
        self.client.put_object(
            Body=csv_buffer.getvalue().encode("utf-8"),
            Bucket=settings.s3_bucket_key,
            Key=publications_path,
            ContentType="text/csv; charset=utf-8",
        )
        logger.info("%s - written %s items", type(self).__name__, df.shape[0])

        metadata_path = (directory_path / f"metadata_{unitname}.json").as_posix()
        metadata_content = create_metadata_content(checksum)
        self.client.put_object(
            Body=metadata_content,
            Bucket=settings.s3_bucket_key,
            Key=metadata_path,
        )
        logger.info("%s - written metadata.json", type(self).__name__)

        yield from items_sorted_by_year
