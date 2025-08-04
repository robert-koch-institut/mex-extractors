from mex.common.fields import ALL_MODEL_CLASSES_BY_NAME, REFERENCE_FIELDS_BY_CLASS_NAME
from mex.common.types import MERGED_IDENTIFIER_CLASSES
from mex.common.utils import contains_any_types, get_all_fields

# TODO(ND): move the contents of this file to mex.common.fields

# allowed entity types grouped for reference fields
REFERENCED_ENTITY_TYPES_BY_FIELD_BY_CLASS_NAME = {
    class_name: {
        field_name: sorted(
            identifier_class.__name__.removesuffix("Identifier")
            for identifier_class in MERGED_IDENTIFIER_CLASSES
            if contains_any_types(
                get_all_fields(ALL_MODEL_CLASSES_BY_NAME[class_name])[field_name],
                identifier_class,
            )
        )
        for field_name in field_names
    }
    for class_name, field_names in REFERENCE_FIELDS_BY_CLASS_NAME.items()
}
