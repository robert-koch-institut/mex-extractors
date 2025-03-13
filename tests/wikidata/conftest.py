import pytest

from mex.common.models import OrganizationMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def organization_mapping() -> OrganizationMapping:
    """Return wikidata organization mapping default values."""
    settings = Settings.get()
    return OrganizationMapping.model_validate(
        load_yaml(settings.wikidata.mapping_path / "organization.yaml")
    )
