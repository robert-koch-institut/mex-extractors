from mex.common.settings import BaseSettings
from mex.extractors.publisher.load import write_merged_items


def test_write_merged_items(ndjson_path) -> None:
    content = [{"Test": 1, "AnotherTest": 2}, {"bla": "blub", "foo": "bar"}]
    write_merged_items(content)

    settings = BaseSettings.get()
    ndjson_path = settings.work_dir / "publisher.ndjson"
    assert (
        ndjson_path.read_text(encoding="utf-8")
        == '{"Test": 1, "AnotherTest": 2}\n{"bla": "blub", "foo": "bar"}\n'
    )
