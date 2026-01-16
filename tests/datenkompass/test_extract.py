import pytest

import mex.extractors.datenkompass.extract as extract_module
from mex.common.models import MergedActivity
from mex.common.types import Text


@pytest.mark.usefixtures("mocked_backend_datenkompass")
def test_get_merged_items_mocked() -> None:
    item_generator = extract_module.get_merged_items(entity_type=["MergedActivity"])
    items = list(item_generator)
    assert len(items) == 3  # 3 mocked MergedActivites
    assert isinstance(items[1], MergedActivity)
    assert items[1] == MergedActivity(
        contact=["LoremIpsum3456"],
        responsibleUnit=["IdentifierUnitPRNT"],
        title=[
            Text(value="titel de", language="de"),
            Text(value="title en", language="en"),
        ],
        abstract=[Text(value="Without language", language=None)],
        end=["1970-01-01"],
        funderOrCommissioner=["Identifier1forORG"],
        shortName=["short only english"],
        theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
        entityType="MergedActivity",
        identifier="MergedActivityWithORG1",
    )


@pytest.mark.usefixtures("mocked_backend_datenkompass", "mocked_provider")
def test_get_filtered_primary_source_ids_mocked() -> None:
    result = extract_module.get_filtered_primary_source_ids(["relevant primary source"])

    assert len(result) == 1
    assert result[0] == "identifierRelevantPS"
