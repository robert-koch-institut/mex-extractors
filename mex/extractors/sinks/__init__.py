from mex.common.sinks.registry import register_sink
from mex.common.types import Sink
from mex.extractors.sinks.base import load
from mex.extractors.sinks.s3 import S3Base

__all__ = ("load",)

register_sink(Sink.S3, S3Base)
