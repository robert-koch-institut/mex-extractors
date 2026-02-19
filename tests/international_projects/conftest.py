from typing import TYPE_CHECKING

import pytest

from mex.common.models import ActivityMapping
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from mex.extractors.settings import Settings


@pytest.fixture
def international_projects_mapping_activity(settings: Settings) -> ActivityMapping:
    return ActivityMapping.model_validate(
        load_yaml(settings.international_projects.mapping_path / "activity_mock.yaml")
    )
