import pytest

from mex.common.models import (
    MergedConsent,
    MergedContactPoint,
    MergedPerson,
    MergedPrimarySource,
)
from mex.extractors.publisher.extract import get_publishable_merged_items


@pytest.mark.usefixtures("mocked_backend")
def test_get_merged_publishable_items_mocked() -> None:
    items = get_publishable_merged_items()
    assert len(items) == 5
    assert items == [
        MergedPrimarySource(
            identifier="fakefakefakeJA",
        ),
        MergedContactPoint(
            email=["1fake@e.mail"],
            identifier="alsofakefakefakeJA",
        ),
        MergedConsent(
            identifier="anotherfakefakefakefak",
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            hasDataSubject="fakefakefakefakefakefa",
            isIndicatedAtTime="2014-05-21T19:38:51Z",
        ),
        MergedContactPoint(
            email=["2fake@e.mail"],
            identifier="alsofakefakefakeYO",
        ),
        MergedPerson(
            fullName="Dr. Fake",
            identifier="drdrdrdrdrdrfAKE",
        ),
    ]
