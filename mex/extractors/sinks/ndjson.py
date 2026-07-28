import json
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class NdjsonSink(BaseSink):
    """Sink that writes models as NDJSON to a local file."""

    def __init__(self, output_path: str | None = None) -> None:
        self.output_path = Path(output_path or "publisher.ndjson")

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT]:
        """Write incoming items as NDJSON to the configured output path."""
        with self.output_path.open("a", encoding="utf-8") as handle:
            total_count = 0
            for item in items:
                handle.write(json.dumps(item, sort_keys=True, cls=MExEncoder))
                handle.write("\n")
                total_count += 1
                yield item
        logger.info("%s - written %s items", type(self).__name__, total_count)