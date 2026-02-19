from collections import deque
from typing import TYPE_CHECKING

from mex.common.sinks.registry import get_sink
from mex.extractors.logging import watch_progress

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.models import AnyExtractedModel, AnyMergedModel, AnyRuleSetResponse


def load(
    models: Iterable[AnyExtractedModel | AnyMergedModel | AnyRuleSetResponse],
) -> None:
    """Load models to all of the configured sinks."""
    sink = get_sink()
    deque(sink.load(watch_progress(models, "load")), maxlen=0)  # exhaust the generator
