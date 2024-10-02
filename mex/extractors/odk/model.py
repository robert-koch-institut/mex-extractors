from mex.common.models import BaseModel


class ODKData(BaseModel):
    """Model class for odk data."""

    file_name: str
    hint: dict[str, list[str | float]]
    label_choices: dict[str, list[str | float]]
    label_survey: dict[str, list[str | float]]
    list_name: list[str | float]
    name: list[str | float]
    type: list[str | float]
