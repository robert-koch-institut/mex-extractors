from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.ifsg.connector import IFSGConnector
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

if TYPE_CHECKING:
    from pydantic import BaseModel


@pytest.fixture
def mocked_ifsg(
    mocked_ifsg_sql_tables: dict[type[BaseModel], list[dict[str, Any]]],
    monkeypatch: MonkeyPatch,
) -> None:
    """Mock IFSG connector."""

    def mocked_init(self: IFSGConnector) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = ["mocked"]
        self._connection = MagicMock()
        self._connection.cursor.return_value.__enter__.return_value = cursor

    monkeypatch.setattr(IFSGConnector, "__init__", mocked_init)

    monkeypatch.setattr(
        IFSGConnector,
        "parse_rows",
        lambda self, model: mocked_ifsg_sql_tables[model],
    )


@pytest.fixture
def mocked_ifsg_sql_tables() -> dict[type[BaseModel], list[dict[str, Any]]]:
    return {
        MetaCatalogue2Item: [
            {"IdCatalogue2Item": 0, "IdCatalogue": 0, "IdItem": 0},
            {
                "IdCatalogue2Item": 1,
                "IdCatalogue": 1001,
                "IdItem": 1001,
            },
        ],
        MetaCatalogue2Item2Schema: [
            {
                "IdCatalogue2Item": 1,
            },
            {
                "IdCatalogue2Item": 1,
            },
        ],
        MetaDataType: [{"IdDataType": 0, "DataTypeName": "DummyType"}],
        MetaDisease: [
            {
                "IdType": 101,
                "IdSchema": 1,
                "DiseaseName": "virus",
                "DiseaseNameEN": "Epidemic",
                "SpecimenName": "virus",
                "IfSGBundesland": 0,
                "InBundesland": "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16",
                "ReferenceDefA": 0,
                "ReferenceDefB": 1,
                "ReferenceDefC": 1,
                "ReferenceDefD": 0,
                "ReferenceDefE": 0,
                "ICD10Code": "A1",
            },
            {
                "IdType": 101,
                "IdSchema": 1,
                "DiseaseName": "virus",
                "DiseaseNameEN": "Epidemic",
                "SpecimenName": "virus",
                "IfSGBundesland": 0,
                "InBundesland": "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16",
                "ReferenceDefA": 0,
                "ReferenceDefB": 1,
                "ReferenceDefC": 1,
                "ReferenceDefD": 0,
                "ReferenceDefE": 0,
                "ICD10Code": "A1",
            },
        ],
        MetaField: [
            {
                "IdField": -1,
                "IdType": 0,
                "IdDataType": 0,
                "IdFieldType": 0,
                "Sort": -1,
                "ToTransport": 0,
                "GuiText": "---",
                "GuiTooltip": None,
                "IdCatalogue": 0,
                "StatementAreaGroup": None,
            },
            {
                "IdField": 0,
                "IdType": 0,
                "IdDataType": 0,
                "IdFieldType": 0,
                "Sort": 0,
                "ToTransport": 0,
                "GuiText": "---",
                "GuiTooltip": None,
                "IdCatalogue": 0,
                "StatementAreaGroup": None,
            },
        ],
        MetaItem: [
            {
                "IdItem": 0,
                "ItemName": "NullItem",
                "ItemNameEN": None,
            },
            {
                "IdItem": 1001,
                "ItemName": "-nicht erhoben-",
                "ItemNameEN": "- not enquired -",
            },
        ],
        MetaSchema2Field: [
            {"IdSchema": 10, "IdField": 1},
            {
                "IdSchema": 10,
                "IdField": 2,
            },
        ],
        MetaSchema2Type: [
            {
                "IdSchema": 1,
                "IdType": 0,
            },
            {
                "IdSchema": 1,
                "IdType": 11,
            },
        ],
        MetaType: [
            {"code": "test1", "id_type": 101, "sql_table_name": "Disease71ABC"},
            {"code": "test2", "id_type": 1, "sql_table_name": "Disease"},
        ],
    }
