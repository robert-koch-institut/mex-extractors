from mex.extractors.sumo.models.base import SumoBaseModel


class Cc2FeatProjection(SumoBaseModel):
    """Model class for feat projection."""

    feature_abbr: str
    feature_description: str
    feature_domain: str
    feature_name_en: str
    feature_name_de: str
    feature_subdomain: str
