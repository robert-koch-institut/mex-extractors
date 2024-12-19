from mex.extractors.sumo.models.base import SumoBaseModel


class Cc2AuxMapping(SumoBaseModel):
    """Model class for aux_mapping."""

    sheet_name: str
    column_name: str
    variable_name_column: list[str]
