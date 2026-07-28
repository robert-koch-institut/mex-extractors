
from pathlib import Path
from typing import TYPE_CHECKING

from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.ndjson import NdjsonSink

if TYPE_CHECKING:

    from pytest import MonkeyPatch

    from mex.common.models import ExtractedOrganization


def test_ndjson_load(
    extracted_organization_rki: ExtractedOrganization,
    monkeypatch: MonkeyPatch,
) -> None:
    settings = ExtractorsSettings.get()
    project_root = Path.cwd()
    monkeypatch.setattr(settings, "work_dir", project_root)
    sink = NdjsonSink()
    result = list(sink.load([extracted_organization_rki]))
    assert result == [extracted_organization_rki]

    output_file = project_root / "publisher_items.ndjson"
    assert output_file.exists()

    #output = ("publisher_items.ndjson").read_text()
    output = output_file.read_text(encoding="utf-8")
    assert "ExtractedOrganization" in output