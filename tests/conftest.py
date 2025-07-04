from pathlib import Path
from typing import Any

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    AnyMergedModel,
    ExtractedOrganization,
    PaginatedItemsContainer,
)
from mex.common.types import MergedPrimarySourceIdentifier
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
def mocked_backend(monkeypatch: MonkeyPatch) -> None:
    """Mock the backendAPIConnector to return dummy variables."""

    def mocked_request(
        _self: BackendApiConnector,
        _method: str = "GET",
        _endpoint: str | None = None,
        _payload: Any = None,  # noqa: ANN401
        _params: dict[str, str] | None = None,
        **_kwargs: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        return PaginatedItemsContainer[AnyMergedModel](
            total=0,
            items=[],
        ).model_dump()

    monkeypatch.setattr(BackendApiConnector, "request", mocked_request)
