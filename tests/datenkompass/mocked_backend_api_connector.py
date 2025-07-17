from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api import connector
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    PaginatedItemsContainer,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_bmg,
    mocked_merged_activities,
    mocked_merged_bibliographic_resource,
    mocked_merged_organizational_units,
    mocked_merged_person,
    mocked_preview_primary_sources,
)


@pytest.fixture
def mocked_backend_api_connector(monkeypatch: MonkeyPatch) -> None:
    """Mock the backendAPIConnector to return dummy variables."""

    class FakeConnector:
        def fetch_merged_items(
            self,
            query_string: str | None,  # noqa: ARG002
            entity_type: list[str] | None,
            had_primary_source: list[str] | None,  # noqa: ARG002
            skip: int,  # noqa: ARG002
            limit: int,  # noqa: ARG002
        ) -> PaginatedItemsContainer[AnyMergedModel]:
            if entity_type == ["MergedActivity"]:
                return_items: Sequence[AnyMergedModel] = [mocked_merged_activities()[1]]
            elif entity_type == ["MergedBibliographicResource"]:
                return_items = mocked_merged_bibliographic_resource()
            elif entity_type == ["MergedOrganizationalUnit"]:
                return_items = [mocked_merged_organizational_units()[0]]
            elif entity_type == ["MergedOrganization"]:
                return_items = [mocked_bmg()[1]]
            elif entity_type == ["MergedPerson"]:
                return_items = mocked_merged_person()

            return PaginatedItemsContainer[AnyMergedModel](
                total=3,
                items=return_items,
            )

        def fetch_preview_items(
            self,
            query_string: str | None,  # noqa: ARG002
            entity_type: list[str] | None,  # noqa: ARG002
            had_primary_source: list[str] | None,  # noqa: ARG002
            skip: int,  # noqa: ARG002
            limit: int,  # noqa: ARG002
        ) -> PaginatedItemsContainer[AnyPreviewModel]:
            return PaginatedItemsContainer[AnyPreviewModel](
                total=2,
                items=mocked_preview_primary_sources(),
            )

    def fake_get() -> FakeConnector:
        return FakeConnector()

    monkeypatch.setattr(connector.BackendApiConnector, "get", fake_get)
