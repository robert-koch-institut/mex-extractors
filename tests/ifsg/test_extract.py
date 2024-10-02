import pytest

from mex.extractors.ifsg.extract import (
    extract_sql_table,
)
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type


@pytest.mark.usefixtures("mocked_ifsg")
def test_extract_sql_table() -> None:
    meta_schema2type = extract_sql_table(MetaSchema2Type)
    expected = [
        MetaSchema2Type(id_schema=1, id_type=0),
        MetaSchema2Type(id_schema=1, id_type=11),
    ]
    assert meta_schema2type == expected
