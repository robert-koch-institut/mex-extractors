from typing import TypeVar

import pytest
from pydantic import BaseModel

from mex.common.models import (
    ExtractedActivity,
    ExtractedPerson,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    Text,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml
from mex.extractors.voxco.model import VoxcoVariable

ModelT = TypeVar("ModelT", bound=BaseModel)


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> dict[
    str, MergedOrganizationalUnitIdentifier
]:
    """Mock unit stable target ids."""
    return {"C1": MergedOrganizationalUnitIdentifier.generate(seed=44)}


@pytest.fixture
def voxco_extracted_persons() -> list[ExtractedPerson]:
    """Return an extracted person with static dummy values."""
    return [
        ExtractedPerson(
            email=["test_person@email.de"],
            familyName="Contact",
            givenName="Carla",
            fullName="Contact, Carla",
            identifierInPrimarySource="Carla",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=40),
        )
    ]


@pytest.fixture
def voxco_merged_organization_ids_by_query_string() -> dict[
    str, MergedOrganizationIdentifier
]:
    return {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(42)}


@pytest.fixture
def voxco_resource_mappings(settings: Settings) -> list[ResourceMapping]:
    return [
        ResourceMapping.model_validate(
            load_yaml(settings.voxco.mapping_path / "resource_mock.yaml")
        )
    ]


@pytest.fixture
def voxco_extracted_resources_by_str() -> dict[str, ExtractedResource]:
    return {
        "voxco-plus": ExtractedResource(
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(21),
            identifierInPrimarySource="voxco-plus",
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            theme=["https://mex.rki.de/item/theme-37"],
            title=[Text(value="voxco-Plus", language="de")],
            anonymizationPseudonymization=[
                "https://mex.rki.de/item/anonymization-pseudonymization-2"
            ],
            contact=[MergedOrganizationalUnitIdentifier.generate(22)],
            description=[
                Text(value="Erreger-spezifische Zusatzinformationen", language="de")
            ],
            keyword=[Text(value="Surveillance", language="de")],
            language=["https://mex.rki.de/item/language-1"],
            meshId=[
                "http://id.nlm.nih.gov/mesh/D012140",
                "http://id.nlm.nih.gov/mesh/D012141",
                "http://id.nlm.nih.gov/mesh/D007251",
            ],
            method=[Text(value="Selbstabstriche", language="de")],
            qualityInformation=[Text(value="description", language="de")],
            resourceCreationMethod=[
                "https://mex.rki.de/item/resource-creation-method-2"
            ],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-15"],
            resourceTypeSpecific=[Text(value="Nasenabstrich", language="de")],
            rights=[Text(value="Die Daten", language="de")],
            spatial=[Text(value="Deutschland", language="de")],
            entityType="ExtractedResource",
            unitInCharge=[MergedOrganizationalUnitIdentifier.generate(23)],
        )
    }


@pytest.fixture
def voxco_variables() -> dict[str, list[VoxcoVariable]]:
    return {
        "project_voxco-plus": [
            VoxcoVariable(
                Id=50614,
                DataType="Text",
                Type="Discrete",
                QuestionText="Monat",
                Choices=[
                    "@{Code=1; Text=Januar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
                    "@{Code=1; Text=Februar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
                ],
                Text="Tag",
            )
        ]
    }


@pytest.fixture
def international_projects_extracted_activities() -> list[ExtractedActivity]:
    return [
        ExtractedActivity(
            contact=MergedOrganizationalUnitIdentifier.generate(30),
            responsibleUnit=MergedOrganizationalUnitIdentifier.generate(32),
            title=[Text(value="title", language="de")],
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(31),
            identifierInPrimarySource="2022-006",
        )
    ]
