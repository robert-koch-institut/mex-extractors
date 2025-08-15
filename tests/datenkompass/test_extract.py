import pytest

import mex.extractors.datenkompass.extract as extract_module
from mex.common.models import MergedActivity
from mex.common.types import Text


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_get_merged_items_mocked() -> None:
    item_generator = extract_module.get_merged_items("blah", ["MergedActivity"], None)
    items = list(item_generator)
    assert len(items) == 1
    assert isinstance(items[0], MergedActivity)
    assert items[0] == MergedActivity(
        contact=["LoremIpsum3456"],
        responsibleUnit=["IdentifierOrgUnitEG"],
        title=[
            Text(value="titel de", language="de"),
            Text(value="title en", language="en"),
        ],
        abstract=[Text(value="Without language", language=None)],
        funderOrCommissioner=["Identifier1forBMG"],
        shortName=["short only english"],
        theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
        entityType="MergedActivity",
        identifier="MergedActivityWithBMG1",
    )


@pytest.mark.usefixtures("mocked_backend_datenkompass", "mocked_provider")
def test_get_relevant_primary_source_ids_mocked() -> None:
    result = extract_module.get_relevant_primary_source_ids(["relevant primary source"])

    assert len(result) == 1
    assert result[0] == "identifierRelevantPS"
