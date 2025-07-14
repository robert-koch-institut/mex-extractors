import pytest

from mex.common.models import ActivityMapping, ExtractedPerson
from mex.common.types import MergedPrimarySourceIdentifier
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
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=40),
    )


@pytest.fixture
def ff_projects_activity(settings: Settings) -> ActivityMapping:
    """Return FF Projects mapping default values."""
    return ActivityMapping.model_validate(
        load_yaml(settings.ff_projects.mapping_path / "activity_mock.yaml")
    )
