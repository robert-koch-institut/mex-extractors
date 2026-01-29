from pydantic import Field

from mex.common.models import BaseModel


class KVISVariables(BaseModel):
    """Model class for KVIS Variables Table rows."""

    file_type: str = Field(..., alias="FileType")
    datatype_description: str = Field(..., alias="DatatypeDescription")
    field_description: str = Field(..., alias="FieldDescription")
    field_name_short: str = Field(..., alias="FieldNameShort")
    field_name_long: str = Field(..., alias="FieldNameLong")
    fvlist_name: str | None = Field(..., alias="FVListName")


class KVISFieldValues(BaseModel):
    """Model class for KVIS FieldValues Table rows."""

    field_value_list_name: str = Field(..., alias="FieldValueListName")
    field_value: str = Field(..., alias="FieldValue")
    field_value_long_text: str = Field(..., alias="FieldValueLongText")
