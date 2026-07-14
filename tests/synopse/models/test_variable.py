from typing import Any

import numpy as np
import pytest

from mex.extractors.synopse.models.variable import Variablenuebersicht


@pytest.mark.parametrize(
    "variable",
    [
        {
            "textbox49": -97,
            "originalfrage": None,
            "studie_id1": "STUDY1",
            "studie_id2": 12345,
            "symopse_id": 1,
            "textbox51": "Nicht erhoben",
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "val_instrument": None,
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "int_var": False,
            "KeepVarname": False,
        },
        {
            "textbox49": np.nan,
            "originalfrage": None,
            "studie_id1": "STUDY1",
            "studie_id2": 12345,
            "symopse_id": 1,
            "textbox51": np.nan,
            "textbox5": "Krankheiten (1101)",
            "textbox2": "Krankheiten allgemein (110100)",
            "val_instrument": None,
            "textbox21": "Angeborene Fehlbildung",
            "textbox24": "KHEfehlb",
            "int_var": False,
            "KeepVarname": False,
        },
    ],
    ids=["all_params", "missing_auspraegung"],
)
def test_synopse_variable_model(variable: dict[str, Any]) -> None:
    syn_var = Variablenuebersicht.model_validate(variable)
    assert not any(v is np.nan for v in syn_var.model_dump().values())  # noqa: PLW0177
