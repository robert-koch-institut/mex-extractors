from typing import Annotated

from pydantic import BaseModel, Field

from mex.common.models.base.mapping import BaseMapping


class MappingOrFilterRule(BaseModel, extra="forbid"):
    """Generic mapping rule model."""

    forValues: Annotated[list[str] | None, Field(title="forValues")] = None
    setValues: Annotated[list[str] | str | None, Field(title="setValues")] = None
    rule: Annotated[str | None, Field(title="rule")] = None
    expectedOutputExample: Annotated[
        str | None, Field(title="expectedOutputExample")
    ] = None
    forPrimarySource: Annotated[str | None, Field(title="forPrimarySource")] = None
    comment: Annotated[str | None, Field(title="comment")] = None


class DatenkompassMappingField(BaseMapping, extra="forbid"):
    """Model subclass for each datenkompass field mapping."""

    fieldInTarget: Annotated[str, Field(min_length=1, title="fieldInTarget")]
    fieldInMEx: Annotated[str | None, Field(title="fieldInMEx")] = None
    mappingRules: Annotated[
        list[MappingOrFilterRule], Field(min_length=1, title="mappingRules")
    ]
    comment: Annotated[str | None, Field(title="comment")] = None


class DatenkompassFilterField(BaseMapping, extra="forbid"):
    """Model subclass for datenkompass filter."""

    fieldInMEx: Annotated[str, Field(min_length=1, title="fieldInMEx")]
    filterRules: Annotated[
        list[MappingOrFilterRule], Field(min_length=1, title="filterRules")
    ]
    comment: Annotated[str | None, Field(title="comment")] = None


class DatenkompassMapping(BaseMapping, extra="forbid"):
    """A mapping for the Datenkompass mapping yamls."""

    fields: list[DatenkompassMappingField]


class DatenkompassFilter(BaseMapping, extra="forbid"):
    """A mapping for the Datenkompass mapping and filter yamls."""

    fields: list[DatenkompassFilterField]
