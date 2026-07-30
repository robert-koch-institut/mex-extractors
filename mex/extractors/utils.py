from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mex.common.models import ExtractedVariable


def count_outbound_connections(variable: ExtractedVariable) -> int:
    """Count the number of outbound connections for a given ExtractedVariable."""
    count = 0
    for value in (variable.belongsTo, variable.usedIn, variable.hadPrimarySource):
        if value is None:
            continue
        if isinstance(value, list):
            count += sum(item is not None for item in value)
        else:
            count += 1
    return count
