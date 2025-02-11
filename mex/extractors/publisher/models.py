from pydantic import BaseModel

from mex.common.models import AnyMergedModel


class PublisherContainer(BaseModel):
    """Container class for merged items to be published."""

    items: list[AnyMergedModel]
