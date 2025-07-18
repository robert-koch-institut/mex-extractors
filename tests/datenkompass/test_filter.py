from pytest import MonkeyPatch

import mex.extractors.datenkompass.filter as filter_module  # otherw. builtIn is shadowed
from mex.common.models import MergedOrganization
from tests.datenkompass.mocked_item_lists import (
    mocked_bmg,
    mocked_merged_activities,
)


def test_filter_for_bmg(monkeypatch: MonkeyPatch) -> None:
    def fake_get_merged_items(
        query_string: str,  # noqa: ARG001
        entity_type: list[str],  # noqa: ARG001
        had_primary_source: None,  # noqa: ARG001
    ) -> list[MergedOrganization]:
        return mocked_bmg()

    monkeypatch.setattr(filter_module, "get_merged_items", fake_get_merged_items)

    result = filter_module.filter_for_bmg(mocked_merged_activities())

    assert len(result) == 2
    assert result[0].identifier == "MergedActivityWithBMG2"
    assert result[1].identifier == "MergedActivityWithBMG1"
