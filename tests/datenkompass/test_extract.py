import pytest

from mex.common.models import MergedActivity
from mex.extractors.datenkompass.extract import get_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_items_mocked() -> None:
    item_generator = get_merged_items("blah", ["blub"], None)
    items = list(item_generator)
    assert len(items) == 1
    assert isinstance(items[0], MergedActivity)
    assert items == [
        MergedActivity(
            entityType="MergedActivity",
            identifier="fakefakefakeJA",
            contact="12345678901234",
            responsibleUnit="012345678901234",
            title="This is a test",
        ),
    ]
