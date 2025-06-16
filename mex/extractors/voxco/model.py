from pydantic import Field

from mex.common.models import BaseModel


class VoxcoVariable(BaseModel):
    """Model class for Voxco Variable."""

    Id: int
    DataType: str
    Type: str
    QuestionText: str = Field(min_length=0)
    Choices: list[str]
    Text: str = Field(min_length=0)
