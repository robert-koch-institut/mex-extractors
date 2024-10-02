from typing import Any

import pytest

from mex.common.models import ExtractedPerson
from mex.common.types import ActivityType, Identifier, TemporalEntity
from mex.extractors.blueant.models.source import BlueAntSource


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson(
        departmentOrUnit=Identifier("bFQoRhcVH5DHUU"),
        email="samples@rki.de",
        familyName="Sample",
        givenName="Sam",
        wasExtractedFrom=Identifier("bFQoRhcVH5DHVy"),
        identifierInPrimarySource="sam",
        worksFor=Identifier("bFQoRhcVH5DHUY"),
        label="Sample, Sam",
    )


@pytest.fixture
def blueant_source() -> BlueAntSource:
    """Return a sample Blue Ant source."""
    return BlueAntSource(
        end=TemporalEntity(2019, 12, 31),
        name="3_Prototype Space Rocket",
        number="00123",
        start=TemporalEntity(2019, 1, 7),
        client_names="Robert Koch-Institut",
        department="C1",
        projectLeaderEmployeeId="person-567",
        status="Projektumsetzung",
        type_="Standardprojekt",
    )


@pytest.fixture
def blueant_source_without_leader() -> BlueAntSource:
    """Return a sample Blue Ant source without a project leader."""
    return BlueAntSource(
        end=TemporalEntity(2010, 10, 11),
        name="2_Prototype Moon Lander",
        number="00255",
        start=TemporalEntity(2018, 8, 9),
        client_names="Robert Koch-Institut",
        department="C1 Child Department",
        status="Projektumsetzung",
        type_="Sonderforschungsprojekt",
    )


@pytest.fixture
def blueant_activity() -> dict[str, Any]:
    """Return activity default values."""
    return {
        "activityType": [
            {
                "fieldInPrimarySource": "typeId",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": ["typeId: 18426, text: Standardprojekt"],
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": None,
                        "rule": "typeId resolved to text using api endpoint masterdata/projects/types/{typeId}.",
                    },
                    {
                        "forValues": ["03 Drittmittelprojekt"],
                        "setValues": [ActivityType["THIRD_PARTY_FUNDED_PROJECT"]],
                        "rule": None,
                    },
                    {
                        "forValues": [
                            "01 Standardprojekt",
                            "02 Standardprojekt agil",
                            "04 Dienstleistung und Support",
                            "05 Linienprojekt",
                            "06 internes Projekt",
                            "08 Organisationsprojekt",
                            "09 Maßnahme",
                        ],
                        "setValues": [ActivityType["INTERNAL_PROJECT_ENDEAVOR"]],
                        "rule": None,
                    },
                    {
                        "forValues": ["07 Survey"],
                        "setValues": [ActivityType["OTHER"]],
                        "rule": None,
                    },
                ],
                "comment": None,
            }
        ]
    }
