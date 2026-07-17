from types import SimpleNamespace

import pytest

from mex.extractors.pipeline import run_job_in_process
from mex.extractors.synopse import main as synopse_main


@pytest.mark.usefixtures(
    "mocked_ldap",
    "mocked_wikidata",
)
def test_job() -> None:
    assert run_job_in_process("synopse")


def test_synopse_extracted_variables_adds_target_specific_outbound_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MockContext:
        def __init__(self) -> None:
            self.metadata: dict[str, object] = {}

        def add_output_metadata(self, metadata: dict[str, object]) -> None:
            self.metadata = metadata

    def fake_transform(*_args: object, **_kwargs: object) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(
                identifierInPrimarySource="var-1",
                belongsTo=[],
                usedIn=["resource-a"],
            ),
            SimpleNamespace(
                identifierInPrimarySource="var-2",
                belongsTo=[],
                usedIn=["resource-a", "resource-b"],
            ),
        ]

    monkeypatch.setattr(
        synopse_main,
        "transform_synopse_variables_to_mex_variables",
        fake_transform,
    )
    monkeypatch.setattr(synopse_main, "load", lambda *_args, **_kwargs: None)

    context = MockContext()
    synopse_main.synopse_extracted_variables(
        context=context,
        synopse_variables_by_thema={},
        synopse_variable_groups_by_identifier_in_primary_source={},
        synopse_extracted_resources_by_identifier_in_primary_source={
            "resource-a": SimpleNamespace(),
            "resource-b": SimpleNamespace(),
            "resource-c": SimpleNamespace(),
        },
        synopse_study_overviews=[],
    )

    assert context.metadata["outbound_connections_variable_group"] == {
        "var-1": 0,
        "var-2": 0,
    }
    assert context.metadata["outbound_connections_resource"] == {
        "resource-a": 2,
        "resource-b": 1,
        "resource-c": 0,
    }
