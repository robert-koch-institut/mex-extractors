import pytest

from mex.common.models import ActivityMapping, ExtractedPerson, ExtractedPrimarySource
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPrimarySourceIdentifier,
)
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
def unit_stable_target_ids_by_synonym(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Extract the dummy units and return them grouped by synonyms."""
    organigram_units = extract_organigram_units()
    mex_organizational_units = transform_organigram_units_to_organizational_units(
        organigram_units, extracted_primary_sources["organigram"]
    )
    return get_unit_merged_ids_by_synonyms(mex_organizational_units)


@pytest.fixture
def international_projects_mapping_activity(settings: Settings) -> ActivityMapping:
    return ActivityMapping.model_validate(
        load_yaml(settings.international_projects.mapping_path / "activity_mock.yaml")
    )
