import pytest

from mex.common.models import (
    ConsentMapping,
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedPerson,
    MergedPrimarySourceIdentifier,
    PersonMapping,
    ResourceMapping,
)
from mex.common.types import (
    Identifier,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
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
def mocked_open_data_parent_resource() -> list[OpenDataParentResource]:
    mocked_parent_response = create_mocked_parent_response()
    return [
        OpenDataParentResource.model_validate(mocked_parent_response["hits"]["hits"][0])
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
def mocked_open_data_creator_with_affiliation_to_ignore() -> (
    OpenDataCreatorsOrContributors
):
    """Mock an open data person (creator or contributor)."""
    mocked_parent_response = create_mocked_parent_response()
    return OpenDataCreatorsOrContributors.model_validate(
        mocked_parent_response["hits"]["hits"][1]["metadata"]["creators"][0]
    )


@pytest.fixture
def mocked_open_data_creator_with_processed_affiliation() -> (
    OpenDataCreatorsOrContributors
):
    """Mock an open data person (creator or contributor)."""
    mocked_parent_response = create_mocked_parent_response()
    return OpenDataCreatorsOrContributors.model_validate(
        mocked_parent_response["hits"]["hits"][1]["metadata"]["creators"][1]
    )


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
    """Mock an extracted distribution."""
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
    """Return consent mapping."""
    settings = Settings.get()
    return ConsentMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "consent.yaml")
    )


@pytest.fixture
def mocked_open_data_distribution_mapping() -> DistributionMapping:
    """Return distribution mapping."""
    settings = Settings.get()
    return DistributionMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "distribution.yaml")
    )


@pytest.fixture
def mocked_open_data_parent_resource_mapping() -> ResourceMapping:
    """Return parent resource mapping."""
    settings = Settings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )


@pytest.fixture
def mocked_person_mapping() -> PersonMapping:
    """Return person mapping."""
    settings = Settings.get()
    return PersonMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "person.yaml")
    )
