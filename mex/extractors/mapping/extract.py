from os import PathLike
from typing import Any

import yaml
from pydantic import BaseModel

from mex.common.models import MAPPING_MODEL_BY_EXTRACTED_CLASS_NAME


def extract_mapping_data(
    path: PathLike[str], model_type: type[BaseModel]
) -> dict[str, Any]:
    """Return a mapping model with default values.

    Args:
        path: path to mapping json
        model_type: model type of BaseModel to be extracted

    Returns:
        dict with mapping default value data from specified path
    """
    model = MAPPING_MODEL_BY_EXTRACTED_CLASS_NAME[model_type.__name__]
    with open(path, encoding="utf-8") as f:
        yaml_model = yaml.safe_load(f)
    return model.model_validate(yaml_model).model_dump()
