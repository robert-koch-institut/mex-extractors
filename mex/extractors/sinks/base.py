from collections import deque
from collections.abc import Iterable

from mex.common.models import AnyExtractedModel, AnyMergedModel, AnyRuleSetResponse
from mex.common.sinks.registry import get_sink


def load(
    models: Iterable[AnyExtractedModel | AnyMergedModel | AnyRuleSetResponse],
) -> None:
    """Load models to all of the configured sinks."""
    deque(get_sink().load(models), maxlen=0)  # exhaust the generator
