import pytest

from mex.publisher.extract import get_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_items_mocked() -> None:
    item_generator = get_merged_items()
    items = list(item_generator)

    assert len(items) == 0
