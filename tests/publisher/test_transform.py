import pytest

from mex.common.models import (
    AnyMergedModel,
    ItemsContainer,
    MergedActivity,
    MergedPerson,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.publisher.transform import (
    get_unit_id_per_person,
    update_actor_references_where_needed,
)


@pytest.fixture
def merged_activity_contacts_blocked() -> MergedActivity:
    return MergedActivity(
        identifier="activityMExFallback",
        contact=["thisIdIsBlocked"],
        externalAssociate=["thisIdIsBlocked", "thisIdentifierIsOkay"],
        involvedPerson=["thisIdentifierIsOkay"],
        responsibleUnit=["thisUnitIsResponsible"],
        title=["Activity with MEx contact point Fallback"],
    )


@pytest.fixture
def merged_activity_contacts_with_fallback() -> MergedActivity:
    return MergedActivity(
        identifier="activityUnitFallback",
        contact=["PersonWithFallbackUnit", "PersonWithoutFallback"],
        externalAssociate=[
            "thisIdIsBlocked",
            "thisIdentifierIsOkay",
            "PersonWithFallbackUnit",
        ],
        involvedPerson=["thisIdentifierIsOkay"],
        responsibleUnit=["thisUnitIsResponsible"],
        title=["Activity with Unit ID Fallback"],
    )


def test_get_unit_id_per_person(
    merged_ldap_person_list: list[MergedPerson],
    merged_unit_contactpoint_container: ItemsContainer[AnyMergedModel],
) -> None:
    assert get_unit_id_per_person(
        merged_ldap_person_list, merged_unit_contactpoint_container
    ) == {"PersonWithFallbackUnit": ["ValidUnitWithEmail"]}


def test_update_actor_references_where_needed_with_mex_contact_fallback(
    merged_activity_contacts_blocked: MergedActivity,
) -> None:
    update_actor_references_where_needed(
        merged_activity_contacts_blocked,
        allowed_actors=[
            MergedPersonIdentifier("thisIdentifierIsOkay"),
            MergedPersonIdentifier("thisIdWouldBeOkayToo"),
        ],
        fallback_contact_identifiers=[
            MergedContactPointIdentifier("thisIsTheFallbackId")
        ],
        fallback_unit_identifiers_by_person={
            MergedPersonIdentifier("PersonWithFallbackUnit"): [
                MergedOrganizationalUnitIdentifier("ValidUnitWithEmail")
            ]
        },
    )
    assert merged_activity_contacts_blocked.model_dump(
        exclude_defaults=True, mode="json"
    ) == {
        "identifier": "activityMExFallback",
        # contact fallback applied to MEx contact point
        "contact": ["thisIsTheFallbackId"],
        # externalAssociate is filtered to exclude invalid references
        "externalAssociate": ["thisIdentifierIsOkay"],
        # involvedPerson not updated because identifier not blocked
        "involvedPerson": ["thisIdentifierIsOkay"],
        # responsibleUnit not updated because not relating to persons
        "responsibleUnit": ["thisUnitIsResponsible"],
        "title": [
            {"value": "Activity with MEx contact point Fallback", "language": "en"}
        ],
    }


def test_update_actor_references_where_needed_with_unit_fallback(
    merged_activity_contacts_with_fallback: MergedActivity,
) -> None:
    update_actor_references_where_needed(
        merged_activity_contacts_with_fallback,
        allowed_actors=[
            MergedPersonIdentifier("thisIdentifierIsOkay"),
            MergedPersonIdentifier("thisIdWouldBeOkayToo"),
            MergedOrganizationalUnitIdentifier("ValidUnitWithEmail"),
        ],
        fallback_contact_identifiers=[
            MergedContactPointIdentifier("thisIsTheFallbackId")
        ],
        fallback_unit_identifiers_by_person={
            MergedPersonIdentifier("PersonWithFallbackUnit"): [
                MergedOrganizationalUnitIdentifier("ValidUnitWithEmail")
            ]
        },
    )
    assert merged_activity_contacts_with_fallback.model_dump(
        exclude_defaults=True, mode="json"
    ) == {
        "identifier": "activityUnitFallback",
        # contact fallback applied to unit with email
        "contact": ["ValidUnitWithEmail"],
        # externalAssociate is just filtered, because no unit IDs allowed
        "externalAssociate": ["thisIdentifierIsOkay"],
        # involvedPerson not updated because identifier not blocked
        "involvedPerson": ["thisIdentifierIsOkay"],
        # responsibleUnit not updated because not relating to persons
        "responsibleUnit": ["thisUnitIsResponsible"],
        "title": [{"value": "Activity with Unit ID Fallback", "language": "en"}],
    }
