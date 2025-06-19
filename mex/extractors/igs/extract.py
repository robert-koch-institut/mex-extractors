from mex.extractors.igs.connector import IGSConnector
from mex.extractors.igs.model import IGSEnum, IGSProperties, IGSSchemas


def extract_igs_schemas() -> dict[str, IGSSchemas]:
    """Extract IGS schemas.

    Returns:
        IGS schemas by name
    """
    connector = IGSConnector.get()
    raw_json = connector.get_json_from_api()
    schemas = raw_json.get("components", {}).get("schemas", {})
    igs_schemas: dict[str, IGSSchemas] = {}
    for key, value in schemas.items():
        if "enum" in value:
            igs_schemas[key] = IGSEnum(**value)
        if "properties" in value:
            igs_schemas[key] = IGSProperties(**value)
    return igs_schemas
