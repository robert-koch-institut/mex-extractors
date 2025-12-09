from pathlib import Path

import pytest

from mex.common.models import ExtractedOrganization
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.organigram.helpers import _get_cached_unit_merged_ids_by_synonyms
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


@pytest.fixture(autouse=True)
def isolate_unit_cache() -> None:
    # clear the cache to be able to test it.
    _get_cached_unit_merged_ids_by_synonyms.cache_clear()


@pytest.fixture
def extracted_organization_rki() -> ExtractedOrganization:
    return ExtractedOrganization(
        identifierInPrimarySource="Robert Koch-Institut",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(123),
        officialName=["Robert Koch-Institut"],
    )
