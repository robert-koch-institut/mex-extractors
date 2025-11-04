from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from mex.extractors.utils import ensure_list, watch_progress


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, []),
        ([], []),
        ([1, 2, 3], [1, 2, 3]),
        ("hello", ["hello"]),
        ({"key": "value"}, [{"key": "value"}]),
    ],
)
def test_ensure_list(input_value: object, expected: list[object]) -> None:
    result = ensure_list(input_value)
    assert result == expected


def test_watch_progress_yields_all_items() -> None:
    """Test that watch_progress yields all items from the iterable."""
    items = ["item1", "item2", "item3"]
    result = list(watch_progress(items, "test_function"))
    assert result == items


@patch("mex.extractors.utils.logger")
def test_watch_progress_logs_at_intervals(mock_logger: MagicMock) -> None:
    """Test that watch_progress logs at 10000 item intervals."""
    items = list(range(25000))

    result = list(watch_progress(items, "test_function"))

    assert result == items
    assert mock_logger.info.call_count == 3
    mock_logger.info.assert_any_call("%s - %s - %s", 0, "test_function", 0)
    mock_logger.info.assert_any_call("%s - %s - %s", 10000, "test_function", 10000)
    mock_logger.info.assert_any_call("%s - %s - %s", 20000, "test_function", 20000)


@patch("mex.extractors.utils.logger")
def test_watch_progress_logs_first_item(mock_logger: MagicMock) -> None:
    """Test that watch_progress always logs the first item (index 0)."""
    items = ["first", "second"]

    list(watch_progress(items, "test_function"))

    mock_logger.info.assert_called_once_with(
        "%s - %s - %s", 0, "test_function", "first"
    )


def test_watch_progress_works_with_generator() -> None:
    """Test that watch_progress works with generator inputs."""

    def sample_generator() -> Generator[str, None, None]:
        for i in range(5):
            yield f"item_{i}"

    result = list(watch_progress(sample_generator(), "test_generator"))

    assert result == ["item_0", "item_1", "item_2", "item_3", "item_4"]
