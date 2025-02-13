import pytest

from mex.common.models import ActivityMapping, ExtractedPerson
from mex.common.types import Identifier
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.settings import Settings


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
        extract_mapping_data(settings.ff_projects.mapping_path / "activity_mock.yaml")
    )
