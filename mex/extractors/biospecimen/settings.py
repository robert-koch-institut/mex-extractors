from pydantic import Field

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class BiospecimenSettings(BaseModel):
    """Settings submodel for the Biospecimen extractor."""

    raw_data_path: AssetsPath = Field(
        AssetsPath("raw-data/biospecimen"),
        description=(
            "Path to the directory with the biospecimen excel files, "
            "absolute path or relative to `assets_dir`."
        ),
    )
    key_col: str = Field(
        "Feldname",
        description="column name of the biospecimen metadata keys",
    )
    val_col: str = Field(
        "zu extrahierender Wert (maschinenlesbar)",
        description="column name of the biospecimen metadata values",
    )
    mapping_path: AssetsPath = Field(
        AssetsPath("mappings/biospecimen"),
        description=(
            "Path to the directory with the biospecimen mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )
