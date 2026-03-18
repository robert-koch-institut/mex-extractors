from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from mex.common.models import (
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
)
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.types import (
    MergedPrimarySourceIdentifier,
)
from mex.extractors.organigram.helpers import _get_cached_unit_merged_ids_by_synonyms
from mex.extractors.primary_source.helpers import (
    cached_load_extracted_primary_source_by_name,
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks.s3 import S3BaseSink

if TYPE_CHECKING:
    from pytest import MonkeyPatch


pytest_plugins = (
    "mex.common.testing.plugin",
    "tests.blueant.mocked_blueant",
    "tests.confluence_vvt.mocked_confluence_vvt",
    "tests.datscha_web.mocked_datscha_web",
    "tests.drop.mocked_drop",
    "tests.grippeweb.mocked_grippeweb",
    "tests.ifsg.mocked_ifsg",
    "tests.igs.mocked_igs",
    "tests.kvis.mocked_kvis",
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
def isolate_caches() -> None:
    # clear the cache to be able to test it.
    _get_cached_unit_merged_ids_by_synonyms.cache_clear()
    cached_load_extracted_primary_source_by_name.cache_clear()


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
        extracted_organization_rki.stableTargetId,
    )


@pytest.fixture
def mocked_units_by_identifier_in_primary_source(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, ExtractedOrganizationalUnit]:
    return {
        unit.identifierInPrimarySource: unit
        for unit in mocked_extracted_organizational_units
    }


@pytest.fixture
def mocked_s3sink_client(monkeypatch: MonkeyPatch) -> None:
    class MockedBoto:
        def __init__(self) -> None:
            self.put_object: MagicMock = MagicMock(side_effect=self._put_object)
            self.bodies: list[bytes] = []

        def _put_object(self, *_: Any, **kwargs: Any) -> None:  # noqa: ANN401
            body: Any = kwargs.get("Body")

            if isinstance(body, BytesIO):
                self.bodies.append(body.read())
            elif isinstance(body, bytes):
                self.bodies.append(body)
            else:
                msg = f"Unexpected Body type: {type(body)}"
                raise TypeError(msg)

        def close(self) -> None:
            pass

    mocked_client = MockedBoto()

    def mocked_init(self: S3BaseSink) -> None:
        self.client = mocked_client

    monkeypatch.setattr(S3BaseSink, "__init__", mocked_init)
