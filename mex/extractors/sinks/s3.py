import datetime
import hashlib
import json
import re
from importlib import metadata
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

import boto3
import pandas as pd

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import MExError
from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.common.types import UTC
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class S3BaseSink(BaseSink):
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
        settings = Settings.get()
        directory_path = self._build_directory_path()
        items_path = (directory_path / "items.ndjson").as_posix()
        total_count = 0
        with BytesIO() as buffer:
            for item in items:
                item_str = self._convert_item_to_ndjson(item)
                buffer.write(item_str.encode("utf-8"))
                total_count += 1
                yield item
            checksum = self._calculate_checksum(buffer)
            # Reset buffer pointer before uploading
            buffer.seek(0)
            self.client.put_object(
                Body=buffer,
                Bucket=settings.s3_bucket_key,
                Key=items_path,
            )
        logger.info("%s - written %s items", type(self).__name__, total_count)
        metadata_path = (directory_path / "metadata.json").as_posix()
        self._load_metadata(metadata_path, checksum)

    @staticmethod
    def _convert_item_to_ndjson(item: _LoadItemT) -> str:
        """Convert an item to an ndjson string."""
        dumped_json = json.dumps(item, sort_keys=True, cls=MExEncoder)
        return f"{dumped_json}\n"

    @staticmethod
    def _build_directory_path() -> Path:
        """Build directory path that includes the mex-model major and minor version."""
        mex_model_version = metadata.version("mex-model")
        regex_pattern = r"(\d+\.\d+)\..+"
        re_groups = re.match(regex_pattern, mex_model_version)
        if not re_groups:
            msg = (
                f"Cannot parse mex-model version '{mex_model_version}'"
                f" with regex '{regex_pattern}'"
            )
            raise MExError(msg)
        mex_model_major_minor_version = re_groups[1]
        return Path(f"publisher-{mex_model_major_minor_version}")

    @staticmethod
    def _calculate_checksum(buffer: BytesIO) -> str:
        """Calculate sha256 checksum of the buffer."""
        buffer.seek(0)
        return hashlib.sha256(buffer.read()).hexdigest()

    def _load_metadata(self, metadata_path: str, checksum: str) -> None:
        """Write metadata file."""
        settings = Settings.get()
        backend_connector = BackendApiConnector.get()
        response = backend_connector.request("GET", "_system/check")
        backend_version = response["version"]
        versions = {
            "mex-backend": backend_version,
            "mex-common": metadata.version("mex-common"),
            "mex-extractors": metadata.version("mex-extractors"),
            "mex-model": metadata.version("mex-model"),
        }
        payload = {
            "versions": versions,
            "sha256_checksum": checksum,
            "write_completed_at": datetime.datetime.now(tz=UTC).isoformat(),
        }
        payload_json = json.dumps(payload, sort_keys=True, cls=MExEncoder)
        payload_bytes = payload_json.encode(encoding="utf-8")
        self.client.put_object(
            Body=payload_bytes,
            Bucket=settings.s3_bucket_key,
            Key=metadata_path,
        )


class S3XlsxSink(S3BaseSink):
    """Special sink to load models as XLSX file into S3 bucket."""

    def load(
        self, items: Iterable[_LoadItemT], *, unit_name: str | None = None
    ) -> Generator[_LoadItemT]:
        """Write the incoming items as an XLSX directly to S3.

        Args:
            items: Iterable of any kind of items
            unit_name: [optional] unit name for excel naming

        Returns:
            Generator for the loaded items
        """
        settings = Settings.get()
        items_list = list(items)

        optional_name_extension = f"_{unit_name.replace(' ', '')}" if unit_name else ""
        file_name = f"{items_list[0].__class__.__name__}{optional_name_extension}.xlsx"

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
