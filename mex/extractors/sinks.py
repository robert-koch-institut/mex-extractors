from collections.abc import Callable, Iterable
from itertools import tee

from mex.common.exceptions import MExError
from mex.common.models import AnyExtractedModel
from mex.common.settings import BaseSettings
from mex.common.sinks.backend_api import post_to_backend_api
from mex.common.sinks.ndjson import write_ndjson
from mex.common.types import Identifier, Sink


def load(models: Iterable[AnyExtractedModel]) -> None:
    """Load models to the backend API or write to NDJSON files.

    Args:
        models: Iterable of extracted models

    Settings:
        sink: Where to load the provided models
    """
    settings = BaseSettings.get()
    func: Callable[[Iterable[AnyExtractedModel]], Iterable[Identifier]]

    for sink, model_gen in zip(
        settings.sink, tee(models, len(settings.sink)), strict=False
    ):
        if sink == Sink.BACKEND:
            func = post_to_backend_api
        elif sink == Sink.NDJSON:
            func = write_ndjson
        else:
            msg = f"Cannot load to {sink}."
            raise MExError(msg)

        for _ in func(model_gen):
            continue  # unpacking the generator
