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
    mocked_merged_contact_point,
    mocked_merged_organizational_units,
    mocked_merged_person,
    mocked_merged_resource,
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
            if entity_type and len(entity_type) > 0:
                key = entity_type[0]
            else:
                pytest.fail("No entity_type given in query to Backend.")

            mock_dispatch = {
                "MergedActivity": lambda: [mocked_merged_activities()[1]],
                "MergedBibliographicResource": mocked_merged_bibliographic_resource,
                "MergedResource": mocked_merged_resource,
                "MergedOrganizationalUnit": lambda: [
                    mocked_merged_organizational_units()[0]
                ],
                "MergedOrganization": lambda: [mocked_bmg()[1]],
                "MergedPerson": mocked_merged_person,
                "MergedContactPoint": mocked_merged_contact_point,
            }
            default_result: list[AnyMergedModel] = []

            return_items = mock_dispatch.get(key, lambda: default_result)()

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
