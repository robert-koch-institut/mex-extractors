from mex.common.models import MERGED_MODEL_CLASSES_BY_NAME
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.common.utils import (
    GenericFieldInfo,
    get_inner_types,
    group_fields_by_class_name,
)


# TODO(ND): move to mex.common.fields
def contains_exact_types(field: GenericFieldInfo, *types: type) -> bool:
    """Return whether a `field`'s annotation contains exactly the given `types`.

    Unions, lists and type annotations are checked for their inner types and only the
    non-`NoneType` types are considered for the type-check.

    Args:
        field: A `GenericFieldInfo` instance
        types: Types to look for in the field's annotation

    Returns:
        Whether the field contains exactly the given types
    """
    if inner_types := list(get_inner_types(field.annotation, include_none=False)):
        return all(type_ in inner_types for type_ in types) and all(
            inner_type in types for inner_type in inner_types
        )
    return False


# TODO(ND): move to mex.common.fields
# lists of fields referencing contact entities grouped by class names
CONTACT_FIELDS_BY_CLASS_NAME = group_fields_by_class_name(
    MERGED_MODEL_CLASSES_BY_NAME,
    lambda field_info: contains_exact_types(
        field_info,
        MergedOrganizationalUnitIdentifier,
        MergedPersonIdentifier,
        MergedContactPointIdentifier,
    ),
)
