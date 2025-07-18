import pytest

from mex.common.models import (
    MergedPrimarySource,
)
from mex.extractors.publisher.extract import get_publishable_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_publishable_items_mocked() -> None:
    items = get_publishable_merged_items()
    assert len(items) == 6
    assert items[0] == MergedPrimarySource(identifier="fakeFakeSource")
