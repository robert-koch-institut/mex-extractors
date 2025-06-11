from pytest import MonkeyPatch

from mex.common.settings import SETTINGS_STORE
from mex.extractors.settings import Settings


def test_settings(monkeypatch: MonkeyPatch) -> None:
    SETTINGS_STORE.reset()
    monkeypatch.setenv("MEX_ARTIFICIAL__SEED", "12")
    settings = Settings.get()
    assert settings.open_data.url == "https://zenodo"
    assert settings.open_data.community_rki == "robertkochinstitut"
