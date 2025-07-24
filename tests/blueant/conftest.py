
from pathlib import Path

import pytest
from pytest import MonkeyPatch

import mex.extractors.pipeline.checks.main as check_main
from mex.common.models import ActivityMapping
from mex.common.types import TemporalEntity
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.pipeline.checks.models.check import AssetCheck, AssetCheckRule
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
        department="C1 Child Department",
        status="Projektumsetzung",
        type_="Sonderforschungsprojekt",
    )


@pytest.fixture
def blueant_activity(settings: Settings) -> ActivityMapping:
    """Return activity default values."""
    return ActivityMapping.model_validate(
        load_yaml(settings.blueant.mapping_path / "activity_mock.yaml")
    )

@pytest.fixture
def mock_settings(monkeypatch: MonkeyPatch) -> None:
    """Mock Settings.get() so it returns a fixed test path."""
    path = Path(__file__).parent / "data"

    class MockSettings:
        all_checks_path = path

        @classmethod
        def get(cls):
            return cls()

    monkeypatch.setattr("mex.extractors.pipeline.checks.main.Settings", MockSettings)

@pytest.fixture
def mock_yaml() -> AssetCheck:
    path = Path(__file__).parent / "data" / "blueant" / "activity.yaml"
    return AssetCheck.model_validate(load_yaml(path))

@pytest.fixture
def mock_load_asset_check_from_settings(monkeypatch: MonkeyPatch, mock_yaml: AssetCheck) -> None:
    monkeypatch.setattr(
        check_main,
        "load_asset_check_from_settings",
        lambda *_: mock_yaml,
    )

@pytest.fixture
def mock_check_model() -> AssetCheck:
    return AssetCheck(
        rules=[
            AssetCheckRule(fail_if="x_items_more_than", value=10, time_frame="1d", target_type=None),
            AssetCheckRule(fail_if="x_items_less_than", value=5, time_frame="1d", target_type=None),
        ]
    )

@pytest.fixture
def mock_get_rule_details_from_model(monkeypatch: MonkeyPatch, mock_check_model: MonkeyPatch) -> None:
    monkeypatch.setattr(
        "mex.extractors.blueant.checks.load_asset_check_from_settings",
        lambda *_: mock_check_model
    )


