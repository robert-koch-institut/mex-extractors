from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mex.common.models import VariableGroupFilter
    from mex.extractors.igs.model import IGSSchema


def filter_igs_schemas(
    igs_schemas: dict[str, IGSSchema], variable_group_filter: VariableGroupFilter
) -> dict[str, IGSSchema]:
    """Filter and return IGS schemas.

    Args:
        igs_schemas: igs schemas
        variable_group_filter: variable group filter rules

    Returns:
        filtered schemas
    """
    return {
        name: schema
        for name, schema in igs_schemas.items()
        if (for_values := variable_group_filter.fields[0].filterRules[0].forValues)
        and name in for_values
    }
