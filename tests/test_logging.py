import logging
from typing import TYPE_CHECKING

from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.logging import log_filter, watch_progress

if TYPE_CHECKING:
    from pytest import LogCaptureFixture


def test_log_filter(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="mex"):
        log_filter(
            "20",
            MergedPrimarySourceIdentifier("10000000001032"),
            "because this and that",
        )
    expected = (
        "[source filtered] reason: because this and that, "
        "had_primary_source: 10000000001032, identifier_in_primary_source: 20"
    )
    assert expected in caplog.text


def test_watch_progress_with_total(caplog: LogCaptureFixture) -> None:
    items = list(range(25000))
    result = list(watch_progress(items, "test_function"))

    assert result == items
    assert "test_function: 10000/25000" in caplog.text
    assert "test_function: done" in caplog.text


def test_watch_progress_with_iterable(caplog: LogCaptureFixture) -> None:
    items = range(25000)
    result = list(watch_progress(items, "test_function"))

    assert result == list(range(25000))
    assert "test_function: 10000" in caplog.text
    assert "test_function: done" in caplog.text
