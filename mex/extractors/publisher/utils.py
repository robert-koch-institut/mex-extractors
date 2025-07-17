# TODO(ND): move to mex.common.utils
def ensure_list(values: object) -> list[object]:
    """Wrap single objects in lists, replace None with [] and return lists untouched."""
    if values is None:
        return []
    if isinstance(values, list):
        return values
    return [values]
