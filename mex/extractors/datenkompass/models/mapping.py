from typing import Annotated

from pydantic import BaseModel, Field


class MappingRule(BaseModel, extra="forbid"):
    """Generic mapping rule model."""

    forValues: Annotated[list[str] | None, Field(title="forValues")] = None
    setValues: Annotated[list[str] | str | None, Field(title="setValues")] = None
    rule: Annotated[str | None, Field(title="rule")] = None
    expectedOutputExample: Annotated[
        str | None, Field(title="expectedOutputExample")
    ] = None
    forPrimarySource: Annotated[str | None, Field(title="forPrimarySource")] = None
    comment: Annotated[str | None, Field(title="comment")] = None


class DatenkompassMappingField(BaseModel, extra="forbid"):
    """Model subclass for each datenkompass field mapping."""

    fieldInTarget: Annotated[str | None, Field(title="fieldInTarget")] = None
    fieldInMEx: Annotated[str | None, Field(title="fieldInMEx")] = None
    mappingRules: Annotated[
        list[MappingRule], Field(min_length=1, title="mappingRules")
    ]
    comment: Annotated[str | None, Field(title="comment")] = None


class DatenkompassMapping(BaseModel, extra="forbid"):
    """A mapping for the Datenkompass mapping yamls."""

    fields: list[DatenkompassMappingField]
