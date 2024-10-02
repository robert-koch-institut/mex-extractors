import logging

from pytest import LogCaptureFixture

from mex.common.types import Identifier
from mex.extractors.logging import log_filter


def test_log_filter(caplog: LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="mex"):
        log_filter(
            "20",
            Identifier("10000000001032"),
            "zuwendungs_oder_auftraggeber [Sonstige] is None or in settings.skip_clients",
        )
    expected = (
        "[source filtered] reason: zuwendungs_oder_auftraggeber [Sonstige] is None or "
        "in settings.skip_clients, "
        "had_primary_source: 10000000001032, identifier_in_primary_source: 20"
    )
    assert expected in caplog.text
