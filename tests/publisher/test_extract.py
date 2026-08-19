import pytest

from mex.common.models import (
    MergedOrganizationalUnit,
    MergedPrimarySource,
)
from mex.extractors.publisher.extract import (
    get_publishable_merged_item_by_identifier,
    get_publishable_merged_items,
)


@pytest.mark.usefixtures("mocked_backend")
def test_get_publishable_merged_items_mocked() -> None:
    items = get_publishable_merged_items()
    assert len(items) == 6
    assert items[0] == MergedPrimarySource(identifier="fakeFakeSource")


@pytest.mark.usefixtures("mocked_backend")
def test_get_publishable_merged_item_by_identifier_mocked(
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    item = get_publishable_merged_item_by_identifier("someIdentifier")
    assert item == mocked_merged_organizational_units[0]
