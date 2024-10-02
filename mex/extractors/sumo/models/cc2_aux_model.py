from mex.extractors.sumo.models.base import SumoBaseModel


class Cc2AuxModel(SumoBaseModel):
    """Model class for aux model."""

    domain: str
    element_description: str
    in_database_static: bool
    variable_name: str
    depends_on_nokeda_variable: str
