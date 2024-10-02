from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID, ExtractedContactPoint
from mex.common.settings import BaseSettings
from mex.common.types import Sink
from mex.extractors import sinks as sinks_module
from mex.extractors.sinks import load


def test_load_multiple(monkeypatch: MonkeyPatch) -> None:
    settings = BaseSettings.get()
    sinks = [Sink.BACKEND, Sink.NDJSON]
    monkeypatch.setattr(settings, "sink", sinks)

    models = [
        ExtractedContactPoint(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="1",
            email=["one@rki.de"],
        ),
        ExtractedContactPoint(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="2",
            email=["two@rki.de"],
        ),
        ExtractedContactPoint(
            hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
            identifierInPrimarySource="3",
            email=["three@rki.de"],
        ),
    ]

    post_to_backend_api = MagicMock(return_value=[m.identifier for m in models])
    write_ndjson = MagicMock(return_value=[m.identifier for m in models])
    monkeypatch.setattr(sinks_module, "post_to_backend_api", post_to_backend_api)
    monkeypatch.setattr(sinks_module, "write_ndjson", write_ndjson)

    load(models)

    assert list(post_to_backend_api.call_args[0][0]) == models
    assert list(write_ndjson.call_args[0][0]) == models


def test_load_unsupported(monkeypatch: MonkeyPatch) -> None:
    settings = BaseSettings.get()
    monkeypatch.setattr(BaseSettings, "__setattr__", object.__setattr__)
    monkeypatch.setattr(settings, "sink", ["bogus-sink"])

    with pytest.raises(MExError, match="Cannot load to bogus-sink."):
        load([])
