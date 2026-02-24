import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.igs.extract import extract_igs_schemas
from mex.extractors.igs.model import IGSInfo, IGSPropertiesSchema, IGSSchema
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def igs_info() -> IGSInfo:
    return IGSInfo(title="test_title", version="test_version")


@pytest.fixture
def igs_endpoint_counts() -> dict[str, str]:
    return {"/test/count": "42", "pathogen_PATHOGEN": "7", "/uploads/count": "5"}


@pytest.fixture
def igs_access_platform_mapping() -> AccessPlatformMapping:
    settings = Settings.get()
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "access-platform.yaml")
    )


@pytest.fixture
def igs_resource_mapping() -> ResourceMapping:
    settings = Settings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "resource.yaml")
    )


@pytest.fixture
def igs_variable_mapping() -> VariableMapping:
    settings = Settings.get()
    return VariableMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "variable.yaml")
    )


@pytest.fixture
def igs_extracted_contact_points_by_mail_str() -> dict[str, ExtractedContactPoint]:
    """Mock IGS actor."""
    return {
        "fictitiousf@rki.de": ExtractedContactPoint(
            email="fictitiousf@rki.de",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
            identifierInPrimarySource="actor 1",
        ),
        "contactc@rki.de": ExtractedContactPoint(
            email="contactc@rki.de",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=43),
            identifierInPrimarySource="actor 2",
        ),
    }


@pytest.fixture
def igs_schemas(
    mocked_igs: None,  # noqa: ARG001
) -> dict[str, IGSSchema]:
    return extract_igs_schemas()


@pytest.fixture
def filtered_igs_schemas(
    mocked_igs: None,  # noqa: ARG001
) -> dict[str, IGSSchema]:
    return {
        "SchemaCreation": IGSPropertiesSchema(
            properties={
                "schemas": {
                    "items": {"$ref": "#/components/schemas/Pathogen"},
                    "title": "test_title",
                    "type": "date",
                    "description": "test_description",
                }
            }
        )
    }


@pytest.fixture
def extracted_access_platform() -> ExtractedAccessPlatform:
    return ExtractedAccessPlatform.model_validate(
        {
            "hadPrimarySource": "cT4pY9osJlUwPx5ODOGLvk",
            "identifierInPrimarySource": "https://igs",
            "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
            "endpointDescription": {
                "language": "en",
                "title": "test title",
                "url": "https://rki.de:4200/api",
            },
            "endpointType": "https://mex.rki.de/item/api-type-1",
            "endpointURL": {"url": "https://rki.de:4100"},
            "contact": ["cGyT8sVLtQTF7vK24LoOk6"],
            "description": [{"value": "test description", "language": "en"}],
            "landingPage": [{"url": "https://rki.de:4100/docs"}],
            "unitInCharge": ["bFQoRhcVH5DHU8"],
        }
    )
