import pytest

from mex.common.models import ActivityMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def ff_projects_activity(settings: Settings) -> ActivityMapping:
    """Return FF Projects mapping default values."""
    return ActivityMapping.model_validate(
        load_yaml(settings.ff_projects.mapping_path / "activity_mock.yaml")
    )
