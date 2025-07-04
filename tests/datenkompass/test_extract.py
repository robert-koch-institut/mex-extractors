import pytest
from pytest import MonkeyPatch

import mex.extractors.datenkompass.extract as extract_module
from mex.common.identity import Identity
from mex.common.models import (
    AnyMergedModel,
    AnyPreviewModel,
    MergedActivity,
    PaginatedItemsContainer,
)
from tests.datenkompass.mocked_item_lists import (
    mocked_merged_activities,
    mocked_preview_primary_sources,
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
        ) -> PaginatedItemsContainer:
            return PaginatedItemsContainer[AnyMergedModel](
                total=3,
                items=mocked_merged_activities(),
            )

    def fake_get() -> FakeConnector:
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


def test_get_relevant_primary_source_ids_mocked(monkeypatch: MonkeyPatch) -> None:
    mocked_preview_ps = mocked_preview_primary_sources()

    class FakeConnector:
        def fetch_preview_items(
            self,
            query_string: str | None,  # noqa: ARG002
            entity_type: list[str] | None,  # noqa: ARG002
            had_primary_source: list[str] | None,  # noqa: ARG002
            skip: int,  # noqa: ARG002
            limit: int,  # noqa: ARG002
        ) -> PaginatedItemsContainer:
            return PaginatedItemsContainer[AnyPreviewModel](
                total=2,
                items=mocked_preview_ps,
            )

    def fake_get() -> FakeConnector:
        return FakeConnector()

    class FakeProvider:
        def fetch(self, stable_target_id: str) -> list[MergedActivity]:
            if stable_target_id == mocked_preview_ps[0].identifier:
                return [
                    Identity(
                        identifier="12345678901234",
                        hadPrimarySource="00000000000000",
                        identifierInPrimarySource="completely irrelevant",
                        stableTargetId="SomeIrrelevantPS",
                    )
                ]
            if stable_target_id == mocked_preview_ps[1].identifier:
                return [
                    Identity(
                        identifier="98765432109876",
                        hadPrimarySource="00000000000000",
                        identifierInPrimarySource="relevant primary source",
                        stableTargetId="identifierRelevantPS",
                    )
                ]
            pytest.fail("wrong mocking of identity provider")

    def fake_get_provider() -> FakeProvider:
        return FakeProvider()

    monkeypatch.setattr(extract_module.BackendApiConnector, "get", fake_get)
    monkeypatch.setattr(extract_module, "get_provider", fake_get_provider)

    result = extract_module.get_relevant_primary_source_ids(["relevant primary source"])

    assert len(result) == 1
    assert result[0] == "identifierRelevantPS"
