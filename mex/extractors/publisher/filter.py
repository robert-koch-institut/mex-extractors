from collections import defaultdict
from typing import TYPE_CHECKING, cast

from mex.common.exceptions import MExError
from mex.common.models import ActivityFilter
from mex.common.organigram.helpers import (
    get_all_descendant_unit_ids,
    get_first_level_child_unit_ids,
)
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.publisher.extract import get_publishable_merged_items
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from mex.common.models import (
        MergedBibliographicResource,
        MergedConsent,
        MergedOrganizationalUnit,
        MergedPerson,
    )
    from mex.common.types import (
        MergedConsentIdentifier,
        MergedPersonIdentifier,
    )


def filter_persons_with_approving_unique_consent(
    person_items: list[MergedPerson], consent_items: list[MergedConsent]
) -> list[MergedPerson]:
    """Filter for persons with approving consent. Fail if a person has > 1 consent.

    Args:
        person_items: list of persons
        consent_items: list of consents

    Raises:
        MExError if any person has more than one consent. The error lists all concerned
                persons and consents.

    Returns:
        list of filtered persons with approving consent.
    """
    person_items_by_id = {person.identifier: person for person in person_items}

    seen_person_ids_with_consent_ids: dict[
        MergedPersonIdentifier, list[MergedConsentIdentifier]
    ] = defaultdict(list)
    collected_persons_with_positive_consent: list[MergedPerson] = []

    for consent in consent_items:
        person_id = consent.hasDataSubject
        seen_person_ids_with_consent_ids[person_id].append(consent.identifier)

        if (
            consent.hasConsentStatus.name == "VALID_FOR_PROCESSING"
            and (person := person_items_by_id.get(person_id)) is not None
        ):
            collected_persons_with_positive_consent.append(person)

    persons_with_serveral_consents = {
        p: c for p, c in seen_person_ids_with_consent_ids.items() if len(c) > 1
    }
    if persons_with_serveral_consents:
        msg = (
            f"The following Merged Persons are referenced by more than one "
            f"Merged Consent: {persons_with_serveral_consents}."
        )
        raise MExError(msg)

    return collected_persons_with_positive_consent


def cluster_and_filter_bibliographic_resources_by_unit(
    merged_bibliographic_resource: list[MergedBibliographicResource],
) -> dict[MergedOrganizationalUnitIdentifier, list[MergedBibliographicResource]]:
    """Sort Bibliographic Resources by unit and filter out 'forbidden' units.

    This function gets all the unpublishable units from the __all__/activity_filter.
    Then it collects all units, which are departments (i.e. direct child units of PRAES)
    Then it sorts all Bibliographic Resources into a dict by department if the
    department or its child units contributed and if that contributing unit is not an
    unpublishable unit.

    Args:
        merged_bibliographic_resource: Merged Bibliographic Resources as list

    Returns:
        dictionary of Bibliographic Resources by allowed units
    """
    settings = ExtractorsSettings.get()
    all_activity_filter_mapping = ActivityFilter.model_validate(
        load_yaml(settings.publisher.mapping_path / "__all__/activity_filter.yaml")
    )
    activity_filter_rule_by_field = {
        field.fieldInPrimarySource: field
        for field in all_activity_filter_mapping.fields
    }
    if activity_filter_rule_by_field["responsibleUnit"].filterRules[0].forValues:
        forbidden_unit_ids = {
            unit_id
            for unit_name in activity_filter_rule_by_field["responsibleUnit"]
            .filterRules[0]
            .forValues
            for unit_id in (get_unit_merged_id_by_synonym(unit_name) or [])
        }
    else:
        msg = "No units set for __all__/activity_filter.yaml"
        raise MExError(msg)

    merged_organisational_units = cast(
        "list[MergedOrganizationalUnit]",
        get_publishable_merged_items(entity_type=["MergedOrganizationalUnit"]),
    )
    unit_praes_stid = next(
        unit.identifier
        for unit in merged_organisational_units
        if unit.identifier == "praes"
    )

    bibliographic_resource_by_department: defaultdict[
        MergedOrganizationalUnitIdentifier, list[MergedBibliographicResource]
    ] = defaultdict(list)

    for department_unit_id_str in get_first_level_child_unit_ids(
        merged_organisational_units, unit_praes_stid
    ):
        department_unit_id: MergedOrganizationalUnitIdentifier = (
            MergedOrganizationalUnitIdentifier(department_unit_id_str)
        )
        descendant_unit_ids = set(
            get_all_descendant_unit_ids(
                merged_organisational_units,
                str(department_unit_id),
            )
        )
        bibliographic_resource_by_department[department_unit_id] = [
            publication
            for publication in merged_bibliographic_resource
            if (
                any(
                    str(unit_id) in descendant_unit_ids
                    for unit_id in publication.contributingUnit
                )
                and not any(
                    unit_id in forbidden_unit_ids
                    for unit_id in publication.contributingUnit
                )
            )
        ]

    return bibliographic_resource_by_department
