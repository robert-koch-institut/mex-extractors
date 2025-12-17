from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    Text,
)
from mex.extractors.grippeweb.connector import GrippewebConnector
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def grippeweb_merged_contact_point_id_by_email() -> dict[
    str, MergedContactPointIdentifier
]:
    return {"contactc@rki.de": MergedContactPointIdentifier.generate(42)}


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> dict[
    str, list[MergedOrganizationalUnitIdentifier]
]:
    """Mock unit stable target ids."""
    return {"C1": [MergedOrganizationalUnitIdentifier.generate(seed=44)]}


@pytest.fixture
def grippeweb_extracted_persons() -> list[ExtractedPerson]:
    """Return an extracted person with static dummy values."""
    return [
        ExtractedPerson(
            email=["test_person@email.de"],
            familyName="Contact",
            givenName="Roland Resolved",
            fullName="Roland Resolved",
            identifierInPrimarySource="Roland Resolved",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=40),
        )
    ]


@pytest.fixture
def grippeweb_merged_organization_ids_by_query_str() -> dict[
    str, MergedOrganizationIdentifier
]:
    return {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(42)}


@pytest.fixture
def grippeweb_access_platform() -> AccessPlatformMapping:
    settings = Settings.get()
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.grippeweb.mapping_path / "access-platform_mock.yaml")
    )


@pytest.fixture
def grippeweb_resource_mappings() -> list[ResourceMapping]:
    settings = Settings.get()
    resource_1 = ResourceMapping.model_validate(
        load_yaml(settings.grippeweb.mapping_path / "resource_mock1.yaml")
    )
    resource_2 = ResourceMapping.model_validate(
        load_yaml(settings.grippeweb.mapping_path / "resource_mock2.yaml")
    )
    return [resource_1, resource_2]


@pytest.fixture
def grippeweb_extracted_access_platform() -> ExtractedAccessPlatform:
    return ExtractedAccessPlatform(
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
        identifierInPrimarySource="primary-source",
        contact=[MergedContactPointIdentifier.generate(seed=234)],
        technicalAccessibility="https://mex.rki.de/item/technical-accessibility-1",
        title=[Text(value="primary-source", language="en")],
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=235)],
    )


@pytest.fixture
def mocked_grippeweb(
    mocked_grippeweb_sql_tables: dict[str, dict[str, list[str | None]]],
    monkeypatch: MonkeyPatch,
) -> None:
    """Mock grippeweb connector."""

    def mocked_init(self: GrippewebConnector) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = ["mocked"]
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(GrippewebConnector, "__init__", mocked_init)

    monkeypatch.setattr(
        GrippewebConnector,
        "parse_columns_by_column_name",
        lambda self, model: mocked_grippeweb_sql_tables[model],
    )


@pytest.fixture
def mocked_grippeweb_sql_tables() -> dict[str, dict[str, list[str | None]]]:
    return {
        "vActualQuestion": {
            "Id": ["AAA", "BBB"],
            "StartedOn": ["2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000"],
            "FinishedOn": [
                "2023-12-01 00:00:00.0000000",
                "2024-01-01 00:00:00.0000000",
            ],
            "RepeatAfterDays": ["1", "2"],
        },
        "vWeeklyResponsesMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
        "vMasterDataMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
    }


@pytest.fixture
def grippeweb_variable_group() -> VariableGroupMapping:
    settings = Settings.get()
    return VariableGroupMapping.model_validate(
        load_yaml(settings.grippeweb.mapping_path / "variable-group_mock.yaml")
    )


@pytest.fixture
def grippeweb_variable() -> VariableMapping:
    settings = Settings.get()
    return VariableMapping.model_validate(
        load_yaml(settings.grippeweb.mapping_path / "variable_mock.yaml")
    )


@pytest.fixture
def grippeweb_extracted_parent_resource() -> ExtractedResource:
    return ExtractedResource(
        hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
        identifierInPrimarySource="grippeweb",
        accessRestriction="https://mex.rki.de/item/access-restriction-2",
        accrualPeriodicity="https://mex.rki.de/item/frequency-15",
        contact=[MergedContactPointIdentifier.generate(42)],
        temporal="seit 2011",
        theme=["https://mex.rki.de/item/theme-11"],
        title=[Text(value="GrippeWeb", language="de")],
        anonymizationPseudonymization=[
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        description=[Text(value="GrippeWeb", language="de")],
        icd10code=["J00-J99"],
        keyword=[Text(value="Citizen Science", language="en")],
        language=["https://mex.rki.de/item/language-1"],
        meshId=["http://id.nlm.nih.gov/mesh/D012140"],
        method=[Text(value="Online-Befragung", language="de")],
        methodDescription=[Text(value="Online-Surveillanceintrument", language="de")],
        resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
        resourceTypeSpecific=[
            Text(value="bevölkerungsbasierte Surveillancedaten", language="de")
        ],
        rights=[Text(value="Verfahren", language="de")],
        stateOfDataProcessing=["https://mex.rki.de/item/data-processing-state-1"],
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(42)],
        entityType="ExtractedResource",
    )


@pytest.fixture
def extracted_variable_groups(
    grippeweb_extracted_parent_resource: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    return [
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            identifierInPrimarySource="vActualQuestion",
            containedBy=[grippeweb_extracted_parent_resource.stableTargetId],
            label=[Text(value="Additional Questions", language="en")],
            entityType="ExtractedVariableGroup",
        ),
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            identifierInPrimarySource="vWeeklyResponsesMEx",
            containedBy=[grippeweb_extracted_parent_resource.stableTargetId],
            label=[Text(value="Weekly Responses", language="en")],
            entityType="ExtractedVariableGroup",
        ),
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("grippeweb"),
            identifierInPrimarySource="vMasterDataMEx",
            containedBy=[grippeweb_extracted_parent_resource.stableTargetId],
            label=[Text(value="Master Data", language="en")],
            entityType="ExtractedVariableGroup",
        ),
    ]
