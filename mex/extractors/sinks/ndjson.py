import json
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from mex.common.models import BaseModel
from mex.common.sinks.base import BaseSink
from mex.common.transform import MExEncoder
from mex.extractors.settings import ExtractorsSettings

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

_LoadItemT = TypeVar("_LoadItemT", bound=BaseModel)


class NdjsonSink(BaseSink):
    """Sink that writes models as NDJSON to a local file."""

    def load(self, items: Iterable[_LoadItemT]) -> Generator[_LoadItemT]:
        """Write items as NDJSON to a local file.

        Settings:
            work_dir: Base directory for output files.
        """
        settings = ExtractorsSettings.get()
        output_path = Path(settings.work_dir) / "publisher_items.ndjson"
        with output_path.open("w", encoding="utf-8") as f:
            for item in items:
                f.write(json.dumps(item, sort_keys=True, cls=MExEncoder))
                f.write("\n")
                yield item
