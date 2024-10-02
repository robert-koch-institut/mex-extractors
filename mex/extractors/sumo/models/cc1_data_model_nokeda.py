from mex.extractors.sumo.models.base import SumoBaseModel


class Cc1DataModelNoKeda(SumoBaseModel):
    """Model class for data model NoKeda."""

    domain: str
    domain_en: str
    type_json: str
    element_description: str
    element_description_en: str
    variable_name: str
    element_label: str
    element_label_en: str
