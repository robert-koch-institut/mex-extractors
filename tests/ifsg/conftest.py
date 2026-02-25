from typing import TYPE_CHECKING

import pytest

from mex.common.models import (
    ExtractedResource,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.common.types import (
    MergedPrimarySourceIdentifier,
    MergedResourceIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_datatype import MetaDataType
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from mex.extractors.settings import Settings


@pytest.fixture
def meta_catalogue2item() -> list[MetaCatalogue2Item]:
    return [
        MetaCatalogue2Item(id_catalogue2item=0, id_catalogue=0, id_item=0),
        MetaCatalogue2Item(id_catalogue2item=0, id_catalogue=0, id_item=1),
        MetaCatalogue2Item(id_catalogue2item=1, id_catalogue=1001, id_item=1001),
    ]


@pytest.fixture
def meta_catalogue2item2schema() -> list[MetaCatalogue2Item2Schema]:
    return [MetaCatalogue2Item2Schema(id_catalogue2item=0)]


@pytest.fixture
def meta_datatype() -> list[MetaDataType]:
    return [MetaDataType(id_data_type=0, data_type_name="DummyType")]


@pytest.fixture
def meta_schema2type() -> list[MetaSchema2Type]:
    return [
        MetaSchema2Type(id_schema=1, id_type=101),
        MetaSchema2Type(id_schema=42, id_type=102),
    ]


@pytest.fixture
def meta_schema2field() -> list[MetaSchema2Field]:
    return [
        MetaSchema2Field(id_schema=10, id_field=1),
        MetaSchema2Field(id_schema=10, id_field=2),
    ]


@pytest.fixture
def meta_type() -> list[MetaType]:
    return [
        MetaType(code="ABC", id_type=101, sql_table_name="Disease71ABC"),
        MetaType(code="DEF", id_type=102, sql_table_name="Disease73DEF"),
        MetaType(code="GHI", id_type=103, sql_table_name="Disease73GHI"),
        MetaType(code="DEF", id_type=1, sql_table_name="Disease"),
    ]


@pytest.fixture
def meta_item() -> list[MetaItem]:
    return [
        MetaItem(
            id_item=0,
            item_name="NullItem",
            item_name_en=None,
        ),
        MetaItem(
            id_item=1,
            item_name="NullItem2",
            item_name_en=None,
        ),
        MetaItem(
            id_item=1001,
            item_name="-nicht erhoben-",
            item_name_en="- not enquired -,",
        ),
    ]


@pytest.fixture
def meta_disease() -> list[MetaDisease]:
    return [
        MetaDisease(
            id_type=101,
            id_schema=1,
            disease_name="virus",
            disease_name_en="Epidemic",
            specimen_name="virus",
            ifsg_bundesland=0,
            in_bundesland="01,02",
            reference_def_a=0,
            reference_def_b=1,
            reference_def_c=1,
            reference_def_d=0,
            reference_def_e=0,
            icd10_code="A1",
        ),
        MetaDisease(
            id_type=102,
            id_schema=1,
            disease_name="virus",
            disease_name_en="Epidemic",
            specimen_name="virus",
            ifsg_bundesland=1,
            in_bundesland=None,
            reference_def_a=1,
            reference_def_b=0,
            reference_def_c=0,
            reference_def_d=1,
            reference_def_e=1,
            icd10_code="A1",
        ),
        MetaDisease(
            id_type=103,
            id_schema=1,
            disease_name="virus",
            disease_name_en="Epidemic",
            specimen_name="virus",
            ifsg_bundesland=1,
            in_bundesland="07,09",
            reference_def_a=1,
            reference_def_b=0,
            reference_def_c=0,
            reference_def_d=1,
            reference_def_e=1,
            icd10_code="A1",
        ),
    ]


@pytest.fixture
def meta_field() -> list[MetaField]:
    return [
        MetaField(
            gui_text="Id der Version",
            gui_tool_tip="lokaler",
            id_catalogue=0,
            id_type=101,
            id_data_type=0,
            id_field=1,
            id_field_type=3,
            to_transport=0,
            sort=1,
            statement_area_group="Epi",
        ),
        MetaField(
            gui_text="Guid des Datensatzes",
            gui_tool_tip="globaler",
            id_catalogue=-12,
            id_type=101,
            id_data_type=0,
            id_field=2,
            id_field_type=2,
            to_transport=0,
            sort=2,
            statement_area_group=None,
        ),
    ]


@pytest.fixture
def ifsg_variable_group(settings: Settings) -> VariableGroupMapping:
    return VariableGroupMapping.model_validate(
        load_yaml(settings.ifsg.mapping_path / "variable-group_mock.yaml")
    )


@pytest.fixture
def resource_parent(settings: Settings) -> ResourceMapping:
    return ResourceMapping.model_validate(
        load_yaml(settings.ifsg.mapping_path / "resource_parent_mock.yaml")
    )


@pytest.fixture
def resource_states(settings: Settings) -> list[ResourceMapping]:
    return [
        ResourceMapping.model_validate(
            load_yaml(settings.ifsg.mapping_path / "resource_state_1_mock.yaml")
        ),
        ResourceMapping.model_validate(
            load_yaml(settings.ifsg.mapping_path / "resource_state_2_mock.yaml")
        ),
    ]


@pytest.fixture
def resource_diseases(settings: Settings) -> list[ResourceMapping]:
    return [
        ResourceMapping.model_validate(
            load_yaml(settings.ifsg.mapping_path / "resource_disease_1_mock.yaml")
        ),
        ResourceMapping.model_validate(
            load_yaml(settings.ifsg.mapping_path / "resource_disease_2_mock.yaml")
        ),
    ]


@pytest.fixture
def ifsg_extracted_resource_parent() -> ExtractedResource:
    return ExtractedResource(
        hadPrimarySource="gmbJenBOxZ6AkQAx9Y8nl5",
        identifierInPrimarySource="tbd",
        accessPlatform=[],
        accessRestriction="https://mex.rki.de/item/access-restriction-2",
        accrualPeriodicity="https://mex.rki.de/item/frequency-15",
        alternativeTitle=[Text(value="IfSG Meldedaten", language=TextLanguage.DE)],
        anonymizationPseudonymization=[],
        contact=["bFQoRhcVH5DHU6"],
        contributingUnit=[],
        contributor=[],
        created=None,
        creator=[],
        description=[
            Text(value="Das Infektionsschutzgesetz", language=TextLanguage.DE)
        ],
        distribution=[],
        documentation=[],
        externalPartner=[],
        icd10code=[],
        instrumentToolOrApparatus=[],
        isPartOf=[],
        keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
        language=["https://mex.rki.de/item/language-1"],
        resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
        resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
        rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
        sizeOfDataBasis=None,
        spatial=[Text(value="Deutschland", language=TextLanguage.DE)],
        stateOfDataProcessing=[],
        temporal=None,
        theme=["https://mex.rki.de/item/theme-11"],
        title=[
            Text(
                value="Meldedaten nach Infektionsschutzgesetz (IfSG)",
                language=TextLanguage.DE,
            )
        ],
        unitInCharge=["bFQoRhcVH5DHU7"],
        wasGeneratedBy=None,
    )


@pytest.fixture
def ifsg_extracted_resources_state() -> list[ExtractedResource]:
    return [
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="01",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Schleswig-Holstein", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-11"],
            title=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="02",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Hamburg", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Hamburg", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-11"],
            title=[Text(value="Hamburg", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="07",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Schleswig-Holstein", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-11"],
            title=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="09",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Hamburg", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Hamburg", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-11"],
            title=[Text(value="Hamburg", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
    ]


@pytest.fixture
def ifsg_extracted_resources_disease() -> list[ExtractedResource]:
    return [
        ExtractedResource(
            hadPrimarySource="fU5a2ZiWXu9ItMX7gYuuPv",
            identifierInPrimarySource="resource_disease_101_10",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[Text(value="ABC", language=None)],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=["A1"],
            instrumentToolOrApparatus=[
                Text(value="Falldefinition B", language=TextLanguage.DE),
                Text(value="Falldefinition C", language=TextLanguage.DE),
            ],
            isPartOf=[
                "hWaaedrfn2ammVuBSZL4TD",
                "dwbN9TmQwDrEp6a0qriDIf",
                "dEefZZfVSd6l9Lj8JZqjg",
            ],
            keyword=[
                Text(value="virus", language=None),
                Text(value="Epidemic", language=None),
                Text(value="virus", language=None),
            ],
            language=["https://mex.rki.de/item/language-1"],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-13"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Deutschland", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=[
                "https://mex.rki.de/item/theme-11",
            ],
            title=[Text(value="virus", language=None)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        )
    ]


@pytest.fixture
def ifsg_extracted_variable_groups() -> list[ExtractedVariableGroup]:
    return [
        ExtractedVariableGroup(
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(23),
            identifierInPrimarySource="101_Epi",
            containedBy=[MergedResourceIdentifier.generate(24)],
            label=[
                Text(value="Epidemiologische Informationen", language=TextLanguage.DE)
            ],
        )
    ]
