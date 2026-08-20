import json
from typing import TYPE_CHECKING

from mex.common.transform import MExEncoder
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.ndjson import NdjsonSink

if TYPE_CHECKING:
    from pathlib import Path

    from pytest import MonkeyPatch

    from mex.common.models import ExtractedOrganization


def test_ndjson_load(
    extracted_organization_rki: ExtractedOrganization,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = ExtractorsSettings.get()
    monkeypatch.setattr(settings, "work_dir", tmp_path)
    sink = NdjsonSink()
    result = list(sink.load([extracted_organization_rki]))

    assert result == [extracted_organization_rki]

    output_file = tmp_path / "publisher_items.ndjson"
    assert output_file.exists()

    expected = (
        json.dumps(
            extracted_organization_rki,
            sort_keys=True,
            cls=MExEncoder,
        )
        + "\n"
    )

    assert (tmp_path / "publisher_items.ndjson").read_text(encoding="utf-8") == expected
