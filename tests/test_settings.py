from pytest import MonkeyPatch

from mex.common.settings import SETTINGS_STORE
from mex.extractors.settings import Settings


def test_settings(monkeypatch: MonkeyPatch) -> None:
    SETTINGS_STORE.reset()
    monkeypatch.setenv("MEX_SKIP_PARTNERS", '["foo"]')
    monkeypatch.setenv("MEX_ARTIFICIAL__SEED", "12")
    settings = Settings.get()
    assert settings.skip_partners == ["foo"]
    assert settings.artificial.seed == 12
    assert str(settings.artificial.mesh_file) == str(
        (settings.assets_dir / "raw-data/artificial/asciimesh.bin").as_posix()
    )
