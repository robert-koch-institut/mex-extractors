import pytest

from mex.common.models import (
    AnyMergedModel,
    ItemsContainer,
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.publisher.transform import (
    get_resolved_names,
    get_unit_id_per_person,
    transform_merged_bibliographic_resources_for_csv,
    update_actor_references_where_needed,
)


@pytest.fixture
def merged_activity_contacts_with_contactpoint_fallback() -> MergedActivity:
    return MergedActivity(
        identifier="activityCPointFallback",
        contact=["thisIdIsBlocked"],
        externalAssociate=["thisIdIsBlocked", "thisIdentifierIsOkay"],
        involvedPerson=["thisIdentifierIsOkay"],
        responsibleUnit=["thisUnitIsResponsible"],
        title=["Activity with contact point Fallback"],
    )


@pytest.fixture
def merged_activity_contacts_with_unit_fallback() -> MergedActivity:
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
    ) == {"PersonWithFallbackUnit": ["ValidUnitWithEmail"], "PersonWithoutFallback": []}


def test_update_actor_references_where_needed_with_contactpoint_fallback(
    merged_activity_contacts_with_contactpoint_fallback: MergedActivity,
) -> None:
    update_actor_references_where_needed(
        merged_activity_contacts_with_contactpoint_fallback,
        [
            MergedPersonIdentifier("thisIdentifierIsOkay"),
            MergedPersonIdentifier("thisIdWouldBeOkayToo"),
        ],
        [MergedContactPointIdentifier("thisIsTheFallbackId")],
        {
            MergedPersonIdentifier("PersonWithFallbackUnit"): [
                MergedOrganizationalUnitIdentifier("ValidUnitWithEmail")
            ],
            MergedPersonIdentifier("PersonWithoutFallback"): [],
        },
    )
    assert merged_activity_contacts_with_contactpoint_fallback.model_dump(
        exclude_defaults=True, mode="json"
    ) == {
        "identifier": "activityCPointFallback",
        # contact fallback applied to contact point
        "contact": ["thisIsTheFallbackId"],
        # externalAssociate is filtered to exclude invalid references
        "externalAssociate": ["thisIdentifierIsOkay"],
        # involvedPerson not updated because identifier not blocked
        "involvedPerson": ["thisIdentifierIsOkay"],
        # responsibleUnit not updated because not relating to persons
        "responsibleUnit": ["thisUnitIsResponsible"],
        "title": [{"value": "Activity with contact point Fallback", "language": "en"}],
    }


def test_update_actor_references_where_needed_with_unit_fallback(
    merged_activity_contacts_with_unit_fallback: MergedActivity,
) -> None:
    update_actor_references_where_needed(
        merged_activity_contacts_with_unit_fallback,
        [
            MergedPersonIdentifier("thisIdentifierIsOkay"),
            MergedPersonIdentifier("thisIdWouldBeOkayToo"),
            MergedOrganizationalUnitIdentifier("ValidUnitWithEmail"),
            MergedOrganizationalUnitIdentifier("InvalidUnitNoEmail"),
        ],
        [MergedContactPointIdentifier("thisIsTheFallbackId")],
        {
            MergedPersonIdentifier("PersonWithFallbackUnit"): [
                MergedOrganizationalUnitIdentifier("ValidUnitWithEmail")
            ],
            MergedPersonIdentifier("PersonWithoutFallback"): [],
        },
    )
    assert merged_activity_contacts_with_unit_fallback.model_dump(
        exclude_defaults=True, mode="json"
    ) == {
        "identifier": "activityUnitFallback",
        # contact fallback applied to unit with email
        "contact": ["ValidUnitWithEmail"],
        # externalAssociate is just filtered, because no unit IDs allowed in that field
        "externalAssociate": ["thisIdentifierIsOkay"],
        # involvedPerson not updated because identifier not blocked
        "involvedPerson": ["thisIdentifierIsOkay"],
        # responsibleUnit not updated because not relating to persons
        "responsibleUnit": ["thisUnitIsResponsible"],
        "title": [{"value": "Activity with Unit ID Fallback", "language": "en"}],
    }


def test_get_resolved_names_returns_short_name(
    monkeypatch: pytest.MonkeyPatch,
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    unit = mocked_merged_organizational_units[0]

    monkeypatch.setattr(
        "mex.extractors.publisher.transform.get_publishable_merged_item",
        lambda identifier: unit,
    )

    result = get_resolved_names(unit.identifier, "shortName")

    assert result == "C1"


def test_transform_merged_bibliographic_resources_for_csv(
    monkeypatch: pytest.MonkeyPatch,
    merged_bibliographic_resource_list: list[MergedBibliographicResource],
) -> None:
    def fake_get_resolved_names(identifier: str, field_name: str) -> str:
        resolved_names = {
            ("6rqNvZSApUHlz8GkkVP48", "shortName"): "C1",
            ("hIiJpZXVppHvoyeP0QtAoS", "shortName"): "parent",
            ("PersonIdentifier", "fullName"): "Dr. Test Person",
        }
        return resolved_names[(str(identifier), field_name)]

    monkeypatch.setattr(
        "mex.extractors.publisher.transform.get_resolved_names",
        fake_get_resolved_names,
    )

    result = transform_merged_bibliographic_resources_for_csv(
        {
            MergedOrganizationalUnitIdentifier(
                "hIiJpZXVppHvoyeP0QtAoS"
            ): merged_bibliographic_resource_list[0:2],
        }
    )

    assert result.keys() == {"parent"}
    assert len(result["parent"]) == 2
    assert result["parent"][0].model_dump(exclude_defaults=True, mode="json") == {
        "accessRestriction": Joker(),
        "contributingUnit": ["C1"],
        "creator": ["Dr. Test Person"],
        "doi": None,
        "journal": [],
        "publicationYear": None,
        "publisher": [],
        "title": ["title 1, Unit C1"],
    }
    assert result["parent"][1].model_dump(exclude_defaults=True, mode="json") == {
        "accessRestriction": Joker(),
        "contributingUnit": ["parent"],
        "creator": ["Dr. Test Person"],
        "doi": None,
        "journal": [],
        "publicationYear": "2042",
        "publisher": [],
        "title": ["title 1, Unit Parent"],
    }
