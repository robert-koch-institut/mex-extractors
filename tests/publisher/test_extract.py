import pytest

from mex.common.models import MergedConsent, MergedContactPoint, MergedPrimarySource
from mex.extractors.publisher.extract import get_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_items_mocked() -> None:
    item_generator = get_merged_items()
    items = list(item_generator)
    assert len(items) == 4
    assert isinstance(items[0], MergedPrimarySource)
    assert items == [
        MergedPrimarySource(
            entityType="MergedPrimarySource", identifier="fakefakefakeJA"
        ),
        MergedContactPoint(
            email=["1fake@e.mail"],
            entityType="MergedContactPoint",
            identifier="alsofakefakefakeJA",
        ),
        MergedConsent(
            entityType="MergedConsent",
            identifier="anotherfakefakefakefak",
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            hasDataSubject="fakefakefakefakefakefa",
            isIndicatedAtTime="2014-05-21T19:38:51Z",
        ),
        MergedContactPoint(
            email=["2fake@e.mail"],
            entityType="MergedContactPoint",
            identifier="alsofakefakefakeYO",
        ),
    ]
