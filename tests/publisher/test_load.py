from pathlib import Path

from mex.common.models import MergedContactPoint
from mex.common.settings import BaseSettings
from mex.extractors.publisher.load import write_merged_items


def test_write_merged_items(ndjson_path: Path) -> None:
    content = [
        MergedContactPoint(
            email=["1fake@e.mail"],
            entityType="MergedContactPoint",
            identifier="alsofakefakefakeJA",
        ),
        MergedContactPoint(
            email=["2fake@e.mail"],
            entityType="MergedContactPoint",
            identifier="alsofakefakefakeYO",
        ),
    ]
    write_merged_items(content)

    settings = BaseSettings.get()
    ndjson_path = settings.work_dir / "publisher.ndjson"
    assert (
        ndjson_path.read_text(encoding="utf-8")
        == '{"email": ["1fake@e.mail"], "entityType": "MergedContactPoint", "identifier": "alsofakefakefakeJA"}\n{"email": ["2fake@e.mail"], "entityType": "MergedContactPoint", "identifier": "alsofakefakefakeYO"}\n'
    )
