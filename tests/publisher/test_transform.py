import pytest

from mex.common.models import MergedActivity
from mex.common.types import MergedContactPointIdentifier, MergedPersonIdentifier
from mex.extractors.publisher.transform import update_contact_where_needed


@pytest.fixture
def merged_activity() -> MergedActivity:
    return MergedActivity(
        identifier="activity123456",
        contact=["thisIdIsBlocked"],
        externalAssociate=["thisIdIsBlocked"],
        involvedPerson=["thisIdentifierIsOkay"],
        responsibleUnit=["thisUnitIsResponsible"],
        title=["Activity 123456"],
    )


def test_update_contact_where_needed(merged_activity: MergedActivity) -> None:
    update_contact_where_needed(
        merged_activity,
        allowed_contacts=[
            MergedPersonIdentifier("thisIdentifierIsOkay"),
            MergedPersonIdentifier("thisIdWouldBeOkayToo"),
        ],
        fallback_contact_identifiers=[
            MergedContactPointIdentifier("thisIsTheFallbackId")
        ],
    )
    assert merged_activity.model_dump(exclude_defaults=True, mode="json") == {
        "identifier": "activity123456",
        # contact fallback applied
        "contact": ["thisIsTheFallbackId"],
        # externalAssociate not updated because field does not allow contact points
        "externalAssociate": ["thisIdIsBlocked"],
        # involvedPerson not updated because identifier not blocked
        "involvedPerson": ["thisIdentifierIsOkay"],
        # responsibleUnit not updated because not relating to persons
        "responsibleUnit": ["thisUnitIsResponsible"],
        "title": [{"value": "Activity 123456", "language": "en"}],
    }
