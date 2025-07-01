import json
from collections.abc import Generator, Iterable
from io import BytesIO
from typing import TypeVar

import boto3

from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.common.utils import grouper
from mex.extractors.settings import Settings

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class S3Sink(BaseSink):
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

    def close(self) -> None:
        """Close the underlying boto client."""
        self.client.close()

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT, None, None]:
        """Write the incoming items as an NDJSON directly to S3.

        Args:
            items: Iterable of any kind of items

        Returns:
            Generator for the loaded items
        """
        settings = Settings.get()
        total_count = 0
        with BytesIO() as buffer:
            for chunk in grouper(self.CHUNK_SIZE, items):
                for item in chunk:
                    if item is None:
                        continue
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
