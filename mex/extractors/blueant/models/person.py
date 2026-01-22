from mex.common.models import BaseModel


class BlueAntPerson(BaseModel):
    """Model class for Blue Ant persons."""

    id: int
    personnelNumber: str
    firstname: str
    lastname: str
    email: str


class BlueAntPersonResponse(BaseModel):
    """Response to GET request to Blue Ant API person endpoint."""

    person: BlueAntPerson
