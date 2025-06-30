import pytest
from pytest import MonkeyPatch

import mex.extractors.datenkompass.extract as extract_module
from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import (
    AnyMergedModel,
    MergedActivity,
    PaginatedItemsContainer,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_merged_activities,
)


def test_get_merged_items_mocked(monkeypatch: MonkeyPatch) -> None:
    class FakeConnector:
        def fetch_merged_items(
            self,
            query_string: str | None,  # noqa: ARG002
            entity_type: list[str] | None,  # noqa: ARG002
            had_primary_source: list[str] | None,  # noqa: ARG002
            skip: int,  # noqa: ARG002
            limit: int,  # noqa: ARG002
        ) -> list[MergedActivity]:
            return PaginatedItemsContainer[AnyMergedModel](
                total=3,
                items=mocked_merged_activities(),
            )

    def fake_get() -> BackendApiConnector:
        return FakeConnector()

    monkeypatch.setattr(extract_module.BackendApiConnector, "get", fake_get)

    item_generator = extract_module.get_merged_items("blah", ["blub"], None)
    items = list(item_generator)
    assert len(items) == 3
    assert isinstance(items[0], MergedActivity)
    assert items[2] == MergedActivity(
        contact=["LoremIpsum5678"],
        responsibleUnit=["DolorSitAmetConsec"],
        title="should get filtered out",
        funderOrCommissioner=["NoBMGIdentifier"],
        theme=["https://mex.rki.de/item/theme-1"],  # PUBLIC_HEALTH
        entityType="MergedActivity",
        identifier="MergedActivityNoBMG",
    )


@pytest.mark.usefixtures("mocked_backend")
def test_get_relevant_primary_source_ids_mocked() -> None:
    pass  # TODO(JE): implement
