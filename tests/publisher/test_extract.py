import pytest

from mex.extractors.publisher.extract import get_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_items_mocked() -> None:
    item_generator = get_merged_items()
    items = list(item_generator)
    assert len(items) == 2
    assert isinstance(items[0], dict)
    assert items == [{"Test": 1, "AnotherTest": 2}, {"bla": "blub", "foo": "bar"}]
