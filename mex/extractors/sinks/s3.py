import json
from collections.abc import Generator, Iterable
from typing import TypeVar

import boto3
from botocore.client import Config

from mex.common.logging import logger
from mex.common.models.base.model import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.extractors.settings import Settings

T = TypeVar("T", bound=BaseModel)


class S3Sink(BaseSink):
    """Sink to load models as new-line delimited JSON file into S3 bucket."""

    def __init__(self) -> None:
        """Instantiate a new S3 sink."""
        settings = Settings.get()
        self.client = boto3.client(
            service_name="s3",
            endpoint_url=str(settings.s3_endpoint_url),
            aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
            aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
            config=Config(signature_version="s3"),
        )

    def close(self) -> None:
        """Close the underlying boto client."""
        self.client.close()

    def load(self, models: Iterable[T]) -> Generator[T, None, None]:
        """Write the incoming items as new-line delimited JSON file into S3."""
        settings = Settings.get()
        ndjson_data = "\n".join(json.dumps(m, cls=MExEncoder) for m in models)

        self.client.put_object(
            Body=ndjson_data.encode("utf-8"),
            Bucket=settings.s3_bucket_key,
            Key="publisher.ndjson",
        )

        logger.info("%s merged items were written.", ndjson_data.count("\n") + 1)
        yield from models
