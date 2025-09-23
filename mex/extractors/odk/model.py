from mex.common.models import BaseModel


class ODKData(BaseModel):
    """Model class for odk data."""

    file_name: str
    label_choices: dict[str, list[str | float]]
    label_survey: dict[str, list[str | float]]
    list_name_choices: list[str | float]
    name_choices: list[str | float]
    name_survey: list[str | float]
    type_survey: list[str | float]
