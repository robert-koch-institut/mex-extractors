from typing import TypeVar

from pydantic import BaseModel

from mex.extractors.kvis.connector import KVISConnector

ModelT = TypeVar("ModelT", bound=BaseModel)


def extract_sql_table(model: type[ModelT]) -> list[ModelT]:
    """Extract sql tables and parse them into pydantic models.

    Returns:
        list of parsed pydantic ModelT
    """
    connection = KVISConnector.get()
    return [model.model_validate(row) for row in connection.parse_rows(model)]
