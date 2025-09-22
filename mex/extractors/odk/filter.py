from mex.common.models import VariableFilter
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


def is_invalid_odk_variable(type_row: str | float) -> bool:
    """Check whether type row is a valid odk variable.

    Args:
        type_row: row in a type column

    Returns:
        True if type_row corresponds to invalid variable else False
    """
    settings = Settings.get()
    variable_filter_mapping = VariableFilter.model_validate(
        load_yaml(settings.odk.mapping_path / "variable_filter.yaml")
    )
    invalid_type_row_list = variable_filter_mapping.fields[0].filterRules[0].forValues
    if not invalid_type_row_list:
        msg = "ODK Variable Filter rule not found."
        raise ValueError(msg)
    return type_row in invalid_type_row_list
