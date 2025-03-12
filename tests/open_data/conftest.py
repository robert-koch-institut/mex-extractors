import pytest

from mex.common.models import (
    ConsentMapping,
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedPerson,
    ExtractedResource,
    MergedPrimarySourceIdentifier,
    ResourceMapping,
)
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml
from tests.open_data.mocked_open_data import (
    create_mocked_parent_response,
    create_mocked_version_response,
)


@pytest.fixture
def mocked_parent_resource_reponse() -> list[OpenDataParentResource]:
    mocked_parent_response = create_mocked_parent_response()
    return [
        OpenDataParentResource.model_validate(mocked_parent_response["hits"]["hits"][0])
    ]


@pytest.fixture
def mocked_extracted_parent_resource() -> list[ExtractedResource]:
    return [
        ExtractedResource(
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
            identifierInPrimarySource="Eins",
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            contact=[Identifier.generate(seed=42)],
            theme=["https://mex.rki.de/item/theme-1"],
            title="Dumdidumdidum",
            unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=42)],
        )
    ]


@pytest.fixture
def mocked_open_data_resource_version() -> list[OpenDataResourceVersion]:
    mocked_resource_version = create_mocked_version_response()
    return [
        OpenDataResourceVersion.model_validate(
            mocked_resource_version["hits"]["hits"][0]
        )
    ]


@pytest.fixture
def mocked_open_data_persons() -> list[ExtractedPerson]:
    """Mock an extracted person."""
    return [
        ExtractedPerson(
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="test_id",
            email=["test_person@email.de"],
            familyName=["Muster"],
            fullName=["Muster, Maxi"],
            givenName=["Maxi"],
        )
    ]


@pytest.fixture
def mocked_open_data_contact_point() -> list[ExtractedContactPoint]:
    """Mock the opendata contact point."""
    return [
        ExtractedContactPoint(
            email="email@email.de",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
            identifierInPrimarySource="contact point",
        )
    ]


@pytest.fixture
def mocked_open_data_distribution() -> list[ExtractedDistribution]:
    """Mock an extracted person."""
    return [
        ExtractedDistribution(
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="file_test_id",
            accessRestriction="https://mex.rki.de/item/access-restriction-1",
            issued="1900-01-01",
            title="test title",
        )
    ]


@pytest.fixture
def mocked_open_data_consent_mapping() -> ConsentMapping:
    """Return consent default values."""
    settings = Settings.get()
    return ConsentMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "consent.yaml")
    )


@pytest.fixture
def mocked_open_data_distribution_mapping() -> DistributionMapping:
    """Return distribution default values."""
    settings = Settings.get()
    return DistributionMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "distribution.yaml")
    )


@pytest.fixture
def mocked_open_data_parent_resource_mapping() -> ResourceMapping:
    """Return parent resource default values."""
    settings = Settings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource_parent.yaml")
    )


@pytest.fixture
def mocked_open_data_resource_version_mapping() -> ResourceMapping:
    """Return resource default values."""
    settings = Settings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )
