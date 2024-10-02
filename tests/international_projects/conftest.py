from typing import Any

import pytest

from mex.common.models import ExtractedPerson, ExtractedPrimarySource
from mex.common.organigram.extract import (
    extract_organigram_units,
    get_unit_merged_ids_by_synonyms,
)
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import (
    ActivityType,
    MergedOrganizationalUnitIdentifier,
    MergedPrimarySourceIdentifier,
    Theme,
)


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
def international_projects_mapping_activity() -> dict[str, Any]:
    return {
        "activityType": [
            {
                "comment": None,
                "examplesInPrimarySource": None,
                "fieldInPrimarySource": "Funding type",
                "locationInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["Third party funded"],
                        "rule": None,
                        "setValues": [ActivityType["THIRD_PARTY_FUNDED_PROJECT"]],
                    },
                    {
                        "forValues": ["RKI funded"],
                        "rule": None,
                        "setValues": [ActivityType["INTERNAL_PROJECT_ENDEAVOR"]],
                    },
                    {
                        "forValues": None,
                        "rule": "If the values in primary source do not match "
                        '"Third party funded" or "RKI funded", then match '
                        'to the following default value in "setValues".',
                        "setValues": [ActivityType["OTHER"]],
                    },
                ],
            }
        ],
        "theme": [
            {
                "comment": None,
                "examplesInPrimarySource": None,
                "fieldInPrimarySource": "Activity [1|2 (optional)]",
                "locationInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": [
                            "Crisis management",
                            "Capacity building including trainings",
                            "Supporting global governance structures and " "processes",
                            "Conducting research",
                        ],
                        "rule": "If field is either empty or has another value, "
                        "set the same value as mentioned above in "
                        '"setValues".',
                        "setValues": [Theme["INTERNATIONAL_HEALTH_PROTECTION"]],
                    }
                ],
            },
            {
                "comment": None,
                "examplesInPrimarySource": None,
                "fieldInPrimarySource": "Topic [1|2 (optional)]",
                "locationInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": [
                            "Public health systems",
                            "One Health",
                            "Laboratory diagnostics",
                        ],
                        "rule": "If field is either empty or has another value, "
                        "set the same value as mentioned above in "
                        '"setValues".',
                        "setValues": [Theme["INTERNATIONAL_HEALTH_PROTECTION"]],
                    },
                    {
                        "forValues": ["Non-communciable diseases"],
                        "rule": None,
                        "setValues": [
                            Theme["NON_COMMUNICABLE_DISEASES_AND_HEALTH_SURVEILLANCE"]
                        ],
                    },
                ],
            },
        ],
    }
