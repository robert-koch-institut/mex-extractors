import json
from pathlib import Path
from typing import Any, cast

import pytest

from mex.common.models import ActivityMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture
def detail_page_data_html() -> str:
    """Return dummy detail page HTML."""
    with (TEST_DATA_DIR / "detail_page_data.html").open(encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture
def detail_page_data_json(detail_page_data_html: str) -> dict[str, Any]:
    """Return dummy detail page JSON."""
    with (TEST_DATA_DIR / "detail_page_data.json").open(encoding="utf-8") as fh:
        detail_page = json.load(fh)
    detail_page["body"]["view"]["value"] = detail_page_data_html
    return cast("dict[str, Any]", detail_page)


@pytest.fixture
def confluence_vvt_activity_mapping(settings: Settings) -> ActivityMapping:
    """Return confluence-vvt activity mapping from assets."""
    return ActivityMapping.model_validate(
        load_yaml(settings.confluence_vvt.template_v1_mapping_path / "activity.yaml")
    )
