import pytest

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    Identifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
    TechnicalAccessibility,
    Text,
    TextLanguage,
)
from mex.extractors.settings import Settings
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection
from mex.extractors.utils import load_yaml


@pytest.fixture
def mex_actor_resources() -> ExtractedContactPoint:
    """Return a dummy mex actor resource."""
    return ExtractedContactPoint(
        email="email@email.de",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
        identifierInPrimarySource="contact point",
    )


@pytest.fixture
def mex_actor_access_platform() -> ExtractedPerson:
    """Return a dummy mex actor access platform."""
    return ExtractedPerson(
        familyName="Mustermann",
        fullName="Erika Mustermann",
        givenName="Erika",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
        identifierInPrimarySource="access platform",
    )


@pytest.fixture
def unit_merged_ids_by_synonym() -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Return dummy merged ids for units for testing."""
    return {
        "MF4": MergedOrganizationalUnitIdentifier.generate(seed=45),
        "mf4": MergedOrganizationalUnitIdentifier.generate(seed=45),
        "FG32": MergedOrganizationalUnitIdentifier.generate(seed=47),
        "fg32": MergedOrganizationalUnitIdentifier.generate(seed=47),
        "FG99": MergedOrganizationalUnitIdentifier.generate(seed=49),
        "fg99": MergedOrganizationalUnitIdentifier.generate(seed=49),
        "FG 99": MergedOrganizationalUnitIdentifier.generate(seed=49),
    }


@pytest.fixture
def contact_merged_ids_by_emails() -> dict[str, MergedContactPointIdentifier]:
    """Return dummy merged ids for units for testing."""
    return {"email@email.de": MergedContactPointIdentifier.generate(seed=51)}


@pytest.fixture
def sumo_resources_feat(settings: Settings) -> ResourceMapping:
    """Return feat SumoResource."""
    return ResourceMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "resource_feat_mock.yaml")
    )


@pytest.fixture
def sumo_resources_nokeda(settings: Settings) -> ResourceMapping:
    """Return feat SumoResource."""
    return ResourceMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "resource_nokeda_mock.yaml")
    )


@pytest.fixture
def sumo_access_platform(settings: Settings) -> AccessPlatformMapping:
    """Return Sumo Access Platform."""
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "access-platform_mock.yaml")
    )


@pytest.fixture
def transformed_sumo_access_platform() -> ExtractedAccessPlatform:
    return ExtractedAccessPlatform(
        identifierInPrimarySource="sumo",
        hadPrimarySource=MergedPrimarySourceIdentifier("eOURPBjMZDfyhAVRo5N4mv"),
        title=[Text(value="SUMO Access Platform", language=TextLanguage.EN)],
        technicalAccessibility=TechnicalAccessibility(
            "https://mex.rki.de/item/technical-accessibility-1"
        ),
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=42)],
        contact=[MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHVf")],
    )


@pytest.fixture
def sumo_activity(settings: Settings) -> ActivityMapping:
    """Return Sumo Activity."""
    return ActivityMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "activity_mock.yaml")
    )


@pytest.fixture
def transformed_activity() -> ExtractedActivity:
    """Return Sumo ExtractedActivity."""
    return ExtractedActivity(
        hadPrimarySource=MergedPrimarySourceIdentifier("hBYPjIX6hKi4FtA5ES5i1a"),
        identifierInPrimarySource="https://url.url",
        abstract=[Text(value="Dummy abstract.", language=TextLanguage.DE)],
        activityType=["https://mex.rki.de/item/activity-type-3"],
        alternativeTitle=[],
        contact=[MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHVf")],
        documentation=[],
        end=[],
        externalAssociate=[MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHVp")],
        funderOrCommissioner=[],
        fundingProgram=[],
        involvedPerson=[],
        involvedUnit=[MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHU9")],
        isPartOfActivity=[],
        publication=[],
        responsibleUnit=[MergedOrganizationalUnitIdentifier("bFQoRhcVH5DHVb")],
        shortName=[Text(value="SUMO", language=TextLanguage.DE)],
        start=[],
        theme=["https://mex.rki.de/item/theme-36"],
        title=[Text(value="SUMO Notaufnahmesurveillance", language=TextLanguage.DE)],
        website=[],
    )


@pytest.fixture
def mex_resources_nokeda() -> ExtractedResource:
    """Return Nokeda ExtractedResources."""
    return ExtractedResource(
        hadPrimarySource=Identifier.generate(seed=5),
        identifierInPrimarySource="test_project",
        accessPlatform=[],
        accessRestriction="https://mex.rki.de/item/access-restriction-2",
        accrualPeriodicity="https://mex.rki.de/item/frequency-15",
        contact=[Identifier.generate(seed=5)],
        contributingUnit=[Identifier.generate(seed=5)],
        description=["Echtzeitdaten der Routinedokumenation"],
        externalPartner=[Identifier.generate(seed=5)],
        keyword=["keyword1", "keyword2"],
        meshId=["http://id.nlm.nih.gov/mesh/D004636"],
        publication=["Situationsreport"],
        publisher=[MergedOrganizationIdentifier("bFQoRhcVH5DHU6")],
        resourceCreationMethod=["https://mex.rki.de/item/resource-creation-method-1"],
        resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-14"],
        resourceTypeSpecific=["Daten"],
        rights=[
            "Die Daten sind zweckgebunden und können nicht ohne Weiteres innerhalb des RKI zur Nutzung zur Verfügung gestellt werden."
        ],
        spatial=["Deutschland"],
        stateOfDataProcessing="https://mex.rki.de/item/data-processing-state-2",
        theme=["https://mex.rki.de/item/theme-11"],
        title=["test_project"],
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=42)],
    )


@pytest.fixture
def mex_resources_feat() -> ExtractedResource:
    """Return feat ExtractedResources."""
    return ExtractedResource(
        hadPrimarySource=Identifier.generate(seed=5),
        identifierInPrimarySource="test_project",
        accessRestriction="https://mex.rki.de/item/access-restriction-2",
        accrualPeriodicity="https://mex.rki.de/item/frequency-17",
        contact=[Identifier.generate(seed=5)],
        contributingUnit=[Identifier.generate(seed=5)],
        keyword=["keyword 1", "keyword 2"],
        meshId=["http://id.nlm.nih.gov/mesh/D004636"],
        resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-14"],
        theme=["https://mex.rki.de/item/theme-11"],
        title=["Syndrome"],
        unitInCharge=[MergedOrganizationalUnitIdentifier.generate(seed=42)],
    )


@pytest.fixture
def cc1_data_model_nokeda() -> list[Cc1DataModelNoKeda]:
    """Return data model nokeda variables."""
    return [
        Cc1DataModelNoKeda(
            domain="Datenbereitstellung",
            domain_en="data supply",
            type_json="string",
            element_description="shobidoo",
            element_description_en="shobidoo_en",
            variable_name="nokeda_edis_software",
            element_label="Name des EDIS",
            element_label_en="Name of EDIS",
        )
    ]


@pytest.fixture
def cc1_data_valuesets() -> list[Cc1DataValuesets]:
    """Return data valuesets variables."""
    return [
        Cc1DataValuesets(
            category_label_de="Herzstillstand (nicht traumatisch)",
            category_label_en="Cardiac arrest (non-traumatic)",
            sheet_name="nokeda_cedis",
        )
    ]


@pytest.fixture
def cc2_aux_mapping() -> list[Cc2AuxMapping]:
    """Return aux mapping variables."""
    return [
        Cc2AuxMapping(
            variable_name_column=["0", "1", "2"],
            column_name="aux_age21_min",
            sheet_name="nokeda_age21",
        ),
        Cc2AuxMapping(
            variable_name_column=["001", "002", "003"],
            column_name="aux_cedis_group",
            sheet_name="nokeda_cedis",
        ),
    ]


@pytest.fixture
def cc2_aux_model() -> list[Cc2AuxModel]:
    """Return aux model variables."""
    return [
        Cc2AuxModel(
            depends_on_nokeda_variable="nokeda_age21",
            domain="age",
            element_description="the lowest age in the age group",
            in_database_static=True,
            variable_name="aux_age21_min",
        ),
        Cc2AuxModel(
            depends_on_nokeda_variable="nokeda_cedis",
            domain="disease",
            element_description="Core groups as defined in the CEDIS reporting standard",
            in_database_static=True,
            variable_name="aux_cedis_group",
        ),
    ]


@pytest.fixture
def cc2_aux_valuesets() -> list[Cc2AuxValuesets]:
    """Return aux valuesets variables."""
    return [Cc2AuxValuesets(label_de="Kardiovaskulär", label_en="Cardiovascular")]


@pytest.fixture
def cc2_feat_projection() -> list[Cc2FeatProjection]:
    """Return feat projection variables."""
    return [
        Cc2FeatProjection(
            feature_domain="feat_syndrome",
            feature_subdomain="RSV",
            feature_abbr="1",
            feature_name_en="respiratory syncytial virus, specific",
            feature_name_de="Respiratorisches Syncytial-Virus, spezifisch",
            feature_description="specific RSV-ICD-10 codes",
        )
    ]


@pytest.fixture
def mex_variable_groups_nokeda_aux() -> list[ExtractedVariableGroup]:
    """Return nokeda variable groups."""
    return [
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="age",
            label=Text(value="age", language=TextLanguage.EN),
        ),
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="disease",
            label=Text(value="disease", language=TextLanguage.EN),
        ),
    ]


@pytest.fixture
def mex_variable_groups_model_nokeda() -> list[ExtractedVariableGroup]:
    """Return nokeda variable groups."""
    return [
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="Datenbereitstellung",
            label={"value": "Datenbereitstellung"},
        ),
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="age",
            label={"value": "age"},
        ),
    ]


@pytest.fixture
def mex_variable_groups_model_feat() -> list[ExtractedVariableGroup]:
    """Return model feat variable groups."""
    return [
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="feat_syndrome RSV",
            label={"value": "feat_syndrome RSV"},
        ),
        ExtractedVariableGroup(
            containedBy=Identifier.generate(seed=42),
            hadPrimarySource=Identifier.generate(seed=42),
            identifierInPrimarySource="feat_syndrome RSV",
            label={"value": "feat_syndrome RSV"},
        ),
    ]
