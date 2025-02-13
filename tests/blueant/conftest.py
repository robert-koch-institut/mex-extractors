import pytest

from mex.common.models import ActivityMapping, ExtractedPerson
from mex.common.types import Identifier, TemporalEntity
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.settings import Settings


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
def blueant_activity() -> ActivityMapping:
    """Return activity default values."""
    settings = Settings.get()
    return ActivityMapping.model_validate(
        extract_mapping_data(settings.blueant.mapping_path / "activity_mock.yaml")
    )
