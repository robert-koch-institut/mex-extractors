from pydantic import ConfigDict

from mex.common.models import BaseModel


class RDMOQuestion(BaseModel):
    """Partial model for RDMO question objects."""

    uri: str
    uri_prefix: str


class RDMOValue(BaseModel):
    """Partial model for RDMO values (questionnaire answers)."""

    model_config = ConfigDict(str_min_length=0)

    attribute: int
    text: str | None
    option: int | None


class RDMOOption(BaseModel):
    """Partial model for RDMO options used in multiple-choice questions."""

    key: str
