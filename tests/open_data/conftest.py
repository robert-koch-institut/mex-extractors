import pytest

from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ResourceMapping,
)
from mex.common.types import (
    MergedPrimarySourceIdentifier,
    MergedResourceIdentifier,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
    OpenDataTableSchema,
    OpenDataTableSchemaCategories,
    OpenDataTableSchemaConstraints,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml
from tests.open_data.mocked_open_data import create_mocked_parent_response


@pytest.fixture
def mocked_open_data_parent_resource() -> list[OpenDataParentResource]:
    mocked_parent_response = create_mocked_parent_response()
    return [
        OpenDataParentResource.model_validate(mocked_parent_response["hits"]["hits"][0])
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
def mocked_open_data_extracted_contact_points() -> list[ExtractedContactPoint]:
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
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
            identifierInPrimarySource="file_test_id",
            accessRestriction="https://mex.rki.de/item/access-restriction-1",
            issued="1900-01-01",
            title="test title",
        )
    ]


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
def mocked_open_data_tableschemas() -> list[OpenDataTableSchema]:
    return [
        OpenDataTableSchema(
            name="Lorem1",
            type="string",
            description="lorem 1",
            constraints=OpenDataTableSchemaConstraints(enum=["a", "b"]),
            categories=[
                OpenDataTableSchemaCategories(value="a", label="the letter 'a'"),
                OpenDataTableSchemaCategories(value="b", label="and also 'b'"),
            ],
        ),
        OpenDataTableSchema(
            name="Lorem2",
            type="string",
            description="lorem 2",
            constraints=OpenDataTableSchemaConstraints(enum=["c", "d", "e", "f", "g"]),
            categories=None,
        ),
        OpenDataTableSchema(
            name="Ipsum",
            type="integer",
            description="no constraints and no categories",
            constraints=None,
            categories=None,
        ),
        OpenDataTableSchema(
            name="Dolor",
            type="boolean",
            description="dolor sit amet",
            constraints=None,
            categories=None,
        ),
    ]


@pytest.fixture
def mocked_open_data_tableschemas_by_resource_id(
    mocked_open_data_tableschemas: list[OpenDataTableSchema],
) -> dict[MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]]:
    return {
        MergedResourceIdentifier("LoremIpsumResourceId"): {
            "tableschema_lorem.json": mocked_open_data_tableschemas[0:2],
            "tableschema_ipsum.json": [mocked_open_data_tableschemas[2]],
        },
        MergedResourceIdentifier("DolorResourceId"): {
            "tableschema_dolor.json": [mocked_open_data_tableschemas[3]],
        },
    }
