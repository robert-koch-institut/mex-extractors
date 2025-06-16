from mex.common.models import BaseModel
from mex.common.types import TemporalEntity


class BlueAntClient(BaseModel):
    """Model class for Blue Ant clients."""

    clientId: int


class BlueAntProject(BaseModel):
    """Model class for Blue Ant projects."""

    clients: list[BlueAntClient]
    departmentId: int
    end: TemporalEntity
    name: str
    number: str
    projectLeaderId: int
    start: TemporalEntity
    statusId: int
    typeId: int


class BlueAntProjectResponse(BaseModel):
    """Response to GET request to Blue Ant API projects endpoint."""

    projects: list[BlueAntProject]
