import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedContactPoint,
    ExtractedResource,
    ResourceMapping,
    VariableMapping,
)
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.igs.extract import extract_igs_schemas
from mex.extractors.igs.model import IGSInfo, IGSPropertiesSchema, IGSSchema
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.utils import load_yaml


@pytest.fixture
def igs_info() -> IGSInfo:
    return IGSInfo(title="test_title", version="test_version")


@pytest.fixture
def igs_endpoint_counts() -> dict[str, str]:
    return {"/test/count": "42", "pathogen_PATHOGEN": "7", "/uploads/count": "5"}


@pytest.fixture
def igs_access_platform_mapping() -> AccessPlatformMapping:
    settings = ExtractorsSettings.get()
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "access-platform.yaml")
    )


@pytest.fixture
def igs_resource_mapping() -> ResourceMapping:
    settings = ExtractorsSettings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "resource.yaml")
    )


@pytest.fixture
def igs_variable_mapping() -> VariableMapping:
    settings = ExtractorsSettings.get()
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


@pytest.fixture
def seq_repo_resources() -> list[ExtractedResource]:
    return [
        ExtractedResource.model_validate(
            {
                "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
                "identifierInPrimarySource": "PATHOGEN",
                "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
                "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
                "start": ["2023-08-07"],
                "modified": "2023-08-07",
                "wasGeneratedBy": "fPqFxu76FLQjVxUDSJpb0z",
                "contact": ["c2Yd8aNoLKIf7u6ubTUuc3", "eXA2Qj5pKmI7HXIgcVqCfz"],
                "theme": [
                    "https://mex.rki.de/item/theme-11",
                    "https://mex.rki.de/item/theme-23",
                ],
                "title": [{"value": "LIMS Sample ID test-sample-id (virus XYZ)"}],
                "unitInCharge": ["cjna2jitPngp6yIV63cdi9", "hIiJpZXVppHvoyeP0QtAoS"],
                "accessPlatform": ["gLB9vC2lPMy5rCmuot99xu"],
                "anonymizationPseudonymization": [
                    "https://mex.rki.de/item/anonymization-pseudonymization-2"
                ],
                "contributingUnit": ["cjna2jitPngp6yIV63cdi9"],
                "description": [
                    {"value": "Testbeschreibung", "language": "de"},
                    {"value": "test description", "language": "en"},
                ],
                "healthCategory": ["https://mex.rki.de/item/health-category-1"],
                "keyword": [
                    {"value": "fastc", "language": "de"},
                    {"value": "fastd", "language": "de"},
                    {"value": "virus XYZ"},
                    {"value": "TEST"},
                ],
                "publisher": ["fxIeF3TWocUZoMGmBftJ6x"],
                "qualityInformation": [
                    {"value": "Basepairs: 1", "language": "en"},
                    {"value": "Reads: 2", "language": "en"},
                ],
                "resourceCreationMethod": [
                    "https://mex.rki.de/item/resource-creation-method-4"
                ],
                "resourceTypeGeneral": [
                    "https://mex.rki.de/item/resource-type-general-13"
                ],
                "resourceTypeSpecific": [
                    {"value": "Sequencing Data", "language": "de"},
                    {"value": "Sequenzdaten", "language": "de"},
                ],
                "rights": [{"value": "Example content", "language": "de"}],
                "stateOfDataProcessing": [
                    "https://mex.rki.de/item/data-processing-state-1"
                ],
            }
        )
    ]
