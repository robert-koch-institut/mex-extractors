from pydantic import Field

from mex.common.models import BaseModel


class MetaDisease(BaseModel):
    """Model class for ifsg meta disease entities."""

    icd10_code: str | None = Field(..., alias="ICD10Code")
    id_type: int = Field(..., alias="IdType")
    id_schema: int = Field(..., alias="IdSchema")
    reference_def_a: bool = Field(..., alias="ReferenceDefA")
    reference_def_b: bool = Field(..., alias="ReferenceDefB")
    reference_def_c: bool = Field(..., alias="ReferenceDefC")
    reference_def_d: bool = Field(..., alias="ReferenceDefD")
    reference_def_e: bool = Field(..., alias="ReferenceDefE")
    ifsg_bundesland: bool = Field(..., alias="IfSGBundesland")
    in_bundesland: str | None = Field(..., alias="InBundesland")
    disease_name: str | None = Field(..., alias="DiseaseName")
    disease_name_en: str | None = Field(..., alias="DiseaseNameEN")
    specimen_name: str | None = Field(..., alias="SpecimenName")
