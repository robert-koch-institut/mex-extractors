from typing import TYPE_CHECKING

from mex.common.settings import SETTINGS_STORE
from mex.extractors.settings import ExtractorSettings

if TYPE_CHECKING:
    from pytest import MonkeyPatch


def test_settings(monkeypatch: MonkeyPatch) -> None:
    SETTINGS_STORE.reset()
    monkeypatch.setenv("MEX_ARTIFICIAL__SEED", "12")
    settings = ExtractorSettings.get()
    assert settings.open_data.url == "https://zenodo"
    assert settings.open_data.community_rki == "robertkochinstitut"
