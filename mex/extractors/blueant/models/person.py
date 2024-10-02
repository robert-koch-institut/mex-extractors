from mex.common.models import BaseModel
from mex.common.types import Email


class BlueAntPerson(BaseModel):
    """Model class for Blue Ant persons."""

    id: int
    personnelNumber: str
    firstname: str
    lastname: str
    email: Email


class BlueAntPersonResponse(BaseModel):
    """Response to GET request to Blue Ant API person endpoint."""

    persons: list[BlueAntPerson]
