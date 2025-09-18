from mex.extractors.igs.model import IGSPropertiesSchema, IGSSchema


def filter_creation_schemas(
    igs_schemas: dict[str, IGSSchema],
) -> dict[str, IGSPropertiesSchema]:
    """Filter and return IGS Creation schemas.

    Args:
        igs_schemas: igs schemas

    Returns:
        filter creation schemas
    """
    return {
        name: schema
        for name, schema in igs_schemas.items()
        if (isinstance(schema, IGSPropertiesSchema))
        and name.endswith("Creation")
        and name != "UploadCreation"
    }
