from pathlib import Path

import pytest

from mex.common.models import (
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
)
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings

pytest_plugins = (
    "mex.common.testing.plugin",
    "tests.blueant.mocked_blueant",
    "tests.confluence_vvt.mocked_confluence_vvt",
    "tests.datscha_web.mocked_datscha_web",
    "tests.drop.mocked_drop",
    "tests.grippeweb.mocked_grippeweb",
    "tests.ifsg.mocked_ifsg",
    "tests.igs.mocked_igs",
    "tests.ldap.mocked_ldap",
    "tests.open_data.mocked_open_data",
    "tests.system.mocked_dagster_instance",
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture(autouse=True)
def settings() -> Settings:
    """Load the settings for this pytest session."""
    return Settings.get()


@pytest.fixture
def extracted_organization_rki() -> ExtractedOrganization:
    return ExtractedOrganization(
        identifierInPrimarySource="Robert Koch-Institut",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(123),
        officialName=["Robert Koch-Institut"],
    )


@pytest.fixture
def mocked_extracted_organizational_units(
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedOrganizationalUnit]:
    return transform_organigram_units_to_organizational_units(
        extract_organigram_units(),
        get_extracted_primary_source_id_by_name("organigram"),
        extracted_organization_rki,
    )


@pytest.fixture
def mocked_units_by_identifier_in_primary_source(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, ExtractedOrganizationalUnit]:
    return {
        unit.identifierInPrimarySource: unit
        for unit in mocked_extracted_organizational_units
    }
