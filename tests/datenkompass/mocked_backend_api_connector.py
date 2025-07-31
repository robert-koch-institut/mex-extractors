from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api import connector
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
    PaginatedItemsContainer,
)
from mex.common.models.primary_source import PreviewPrimarySource


@pytest.fixture
def mocked_backend_api_connector(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_bmg: list[MergedOrganization],
    mocked_merged_person: list[MergedPerson],
    mocked_preview_primary_sources: list[PreviewPrimarySource],
) -> None:
    """Mock the backendAPIConnector to return dummy variables."""

    class FakeConnector:
        def fetch_all_merged_items(
            self,
            *,
            query_string: str | None = None,  # noqa: ARG002
            entity_type: list[str] | None = None,
            referenced_identifier: list[str] | None = None,  # noqa: ARG002
            reference_field: str | None = None,  # noqa: ARG002
        ) -> list[AnyMergedModel]:
            if entity_type == ["MergedActivity"]:
                return_items: Sequence[AnyMergedModel] = [mocked_merged_activities[1]]
            elif entity_type == ["MergedBibliographicResource"]:
                return_items = mocked_merged_bibliographic_resource
            elif entity_type == ["MergedOrganizationalUnit"]:
                return_items = [mocked_merged_organizational_units[0]]
            elif entity_type == ["MergedOrganization"]:
                return_items = [mocked_bmg[1]]
            elif entity_type == ["MergedPerson"]:
                return_items = mocked_merged_person

            return list(return_items)

        def fetch_preview_items(  # noqa: PLR0913
            self,
            *,
            query_string: str | None = None,  # noqa: ARG002
            entity_type: list[str] | None = None,  # noqa: ARG002
            referenced_identifier: list[str] | None = None,  # noqa: ARG002
            reference_field: str | None = None,  # noqa: ARG002
            skip: int = 0,  # noqa: ARG002
            limit: int = 100,  # noqa: ARG002
        ) -> PaginatedItemsContainer[AnyPreviewModel]:
            return PaginatedItemsContainer[AnyPreviewModel](
                total=2,
                items=mocked_preview_primary_sources,
            )

    def fake_get() -> FakeConnector:
        return FakeConnector()

    monkeypatch.setattr(connector.BackendApiConnector, "get", fake_get)
