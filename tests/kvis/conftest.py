import pytest
from mex.common.models import ExtractedVariableGroup
from mex.common.types import MergedResourceIdentifier, TextLanguage


@pytest.fixture
def mocked_extracted_resource_id() -> MergedResourceIdentifier:
    return MergedResourceIdentifier.generate(seed=12345)


@pytest.fixture
def mocked_extracted_variable_groups(
    mocked_extracted_resource_id: MergedResourceIdentifier,
) -> list[ExtractedVariableGroup]:
    return [
        ExtractedVariableGroup(
            containedBy= [mocked_extracted_resource_id],
            hadPrimarySource= "eKx0G7GVS8o9v537kCUM3i",
            identifierInPrimarySource= "kvis_file type 1",
            label= [{"language": TextLanguage.DE, "value": "file type 1"}],
        ),
        ExtractedVariableGroup(
            containedBy=[mocked_extracted_resource_id],
            hadPrimarySource="eKx0G7GVS8o9v537kCUM3i",
            identifierInPrimarySource="kvis_another file type",
            label=[{"language": TextLanguage.DE, "value": "another file type"}],
        )
    ]

