import pytest

from mex.common.models import ActivityMapping, ExtractedPerson
from mex.common.types import Identifier
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson(
        email=["fictitiousf@rki.de", "info@rki.de"],
        familyName="Fictitious",
        givenName="Frieda",
        fullName="Dr. Fictitious, Frieda",
        identifierInPrimarySource="frieda",
        hadPrimarySource=Identifier.generate(seed=40),
    )


@pytest.fixture
def ff_projects_activity() -> ActivityMapping:
    """Return FF Projects mapping default values."""
    settings = Settings.get()
    return ActivityMapping.model_validate(
        load_yaml(settings.ff_projects.mapping_path / "activity_mock.yaml")
    )
