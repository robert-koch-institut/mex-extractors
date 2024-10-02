from pydantic import ConfigDict

from mex.common.models import BaseModel
from mex.extractors.rdmo.models.person import RDMOPerson


class RDMOSource(BaseModel):
    """Model class for RDMO sources."""

    model_config = ConfigDict(str_min_length=0)

    id: int
    title: str
    description: str | None = None
    catalog: int | None = None
    parent: int | None = None
    owners: list[RDMOPerson] = []
    question_answer_pairs: dict[str, str] = {}
