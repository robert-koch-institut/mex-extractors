from typing import TYPE_CHECKING

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.logging import watch_progress
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.extractors.datscha_web.models.item import DatschaWebItem


def transform_datscha_web_items_to_mex_activities(
    datscha_web_items: Iterable[DatschaWebItem],
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    organizations_stable_target_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[ExtractedActivity]:
    """Transform datscha-web items to extracted activities.

    Args:
        datscha_web_items: Datscha-web items
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target IDs
        organizations_stable_target_ids_by_query_string: Mapping from org queries
                                                         to org stable target IDs

    Returns:
        List of ExtractedSources
    """
    activities = []
    for datscha_web_item in watch_progress(
        datscha_web_items, "transform_datscha_web_items_to_mex_activities"
    ):
        # lookup units
        involved_unit = (
            get_unit_merged_id_by_synonym(unit_name)
            if (unit_name := datscha_web_item.zentrale_stelle_fuer_die_verarbeitung)
            else None
        )
        responsible_unit = [
            unit_id
            for unit_name in (
                datscha_web_item.liegenschaften_oder_organisationseinheiten_loz
            )
            if (merged_ids := get_unit_merged_id_by_synonym(unit_name))
            for unit_id in merged_ids
        ]
        # lookup actors
        involved_person = person_stable_target_ids_by_query_string[
            datscha_web_item.auskunftsperson  # type: ignore[index]
        ]
        if involved_person:
            contact: list[MergedPersonIdentifier] = involved_person
        else:
            contact: list[MergedOrganizationalUnitIdentifier] = responsible_unit  # type: ignore[no-redef]

        external_associate: list[MergedOrganizationIdentifier] = []
        for partner in datscha_web_item.get_partners():
            if partner:
                if associate := organizations_stable_target_ids_by_query_string.get(
                    partner
                ):
                    external_associate.append(associate)
                elif partner != "None":
                    extracted_organization = ExtractedOrganization(
                        officialName=partner,
                        identifierInPrimarySource=partner,
                        hadPrimarySource=get_extracted_primary_source_id_by_name(
                            "datscha-web"
                        ),
                    )
                    load([extracted_organization])
                    external_associate.append(
                        MergedOrganizationIdentifier(
                            extracted_organization.stableTargetId
                        )
                    )

        activities.append(
            ExtractedActivity(
                abstract=datscha_web_item.kurzbeschreibung,
                activityType="https://mex.rki.de/item/activity-type-6",
                contact=contact,
                externalAssociate=external_associate,
                hadPrimarySource=get_extracted_primary_source_id_by_name("datscha-web"),
                identifierInPrimarySource=str(datscha_web_item.item_id),
                involvedPerson=involved_person,
                involvedUnit=involved_unit,
                responsibleUnit=responsible_unit,
                title=datscha_web_item.bezeichnung_der_verarbeitungstaetigkeit,
            )
        )
    return activities
