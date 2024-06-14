import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from mex.common.transform import MExEncoder


def write_merged_items(ndjson_path: Path, items: Iterable[dict[str, Any]]) -> None:
    """Write the incoming items into a new-line delimited JSON file."""
    with open(ndjson_path, "a+", encoding="utf-8") as file:
        for item in items:
            file.write(json.dumps(item, cls=MExEncoder) + "\n")
