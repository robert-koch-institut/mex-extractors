from pydantic import ConfigDict

from mex.common.models import BaseModel


class RDMOPerson(BaseModel):
    """Model class for RDMO persons."""

    model_config = ConfigDict(str_min_length=0)

    id: int
    username: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
