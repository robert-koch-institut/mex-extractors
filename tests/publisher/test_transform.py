from pytest import MonkeyPatch

from mex.extractors.publisher.transform import update_contact_where_needed


def test_update_contact_where_needed(monkeypatch: MonkeyPatch) -> None:
    update_contact_where_needed()
