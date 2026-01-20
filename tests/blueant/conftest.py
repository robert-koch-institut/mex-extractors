import pytest

from mex.common.models import ActivityMapping
from mex.common.types import TemporalEntity
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


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
        department="C1 Sub-Unit",
        status="Projektumsetzung",
        type_="Sonderforschungsprojekt",
    )

@pytest.fixture
def blueant_source_with_involved_employee() -> BlueAntSource:
    """Return a sample Blue Ant source with involvedEmployeeId."""
    return BlueAntSource(
        end=TemporalEntity(2010, 10, 11),
        name="4_Prototype Saturn Surfer",
        number="00789",
        start=TemporalEntity(2020, 8, 9),
        client_names="Robert Koch-Institut",
        involvedEmployeeId = "person-789",
        department="C1 Outdated",
        status="Projektumsetzung",
        type_="Sonderforschungsprojekt",
    )

@pytest.fixture
def blueant_activity(settings: Settings) -> ActivityMapping:
    """Return activity default values."""
    return ActivityMapping.model_validate(
        load_yaml(settings.blueant.mapping_path / "activity_mock.yaml")
    )
