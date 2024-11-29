from mex.extractors.sumo.models.base import SumoBaseModel


class Cc1DataValuesets(SumoBaseModel):
    """Model class for data valuesets."""

    category_label_de: str
    category_label_en: str | None
    sheet_name: str
