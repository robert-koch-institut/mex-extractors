from pydantic import ConfigDict

from mex.common.models import BaseModel


class SumoBaseModel(BaseModel):
    """Model class for data model NoKeda."""

    model_config = ConfigDict(extra="ignore", coerce_numbers_to_str=True)
