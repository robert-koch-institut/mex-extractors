import json
from collections.abc import Iterable

from mex.common.logging import logger
from mex.common.models import AnyMergedModel
from mex.common.settings import BaseSettings
from mex.common.transform import MExEncoder


def write_merged_items(items: Iterable[AnyMergedModel]) -> None:
    """Write the incoming items into a new-line delimited JSON file."""
    settings = BaseSettings.get()
    ndjson_path = settings.work_dir / "publisher.ndjson"

    logging_counter = 0

    with open(ndjson_path, "w", encoding="utf-8") as file:
        for item in items:
            file.write(json.dumps(item, cls=MExEncoder) + "\n")
            logging_counter += 1

    logger.info("%s merged items were written.", logging_counter)
