from collections.abc import Generator, Iterable
from typing import cast

from mex.biospecimen.models.source import BiospecimenResource
from mex.common.identity import get_provider
from mex.common.logging import watch
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedResource,
)
from mex.common.types import (
    AccessRestriction,
    AnonymizationPseudonymization,
    Identifier,
    Link,
    ResourceTypeGeneral,
    TemporalEntity,
    Theme,
)


@watch
def transform_biospecimen_resource_to_mex_resource(
    biospecimen_resources: Iterable[BiospecimenResource],
    extracted_platform_biospecimen: ExtractedAccessPlatform,
    extracted_platform_report_server: ExtractedAccessPlatform,
    unit_stable_target_ids_by_synonym: dict[str, Identifier],
    mex_persons: Iterable[ExtractedPerson],
    extracted_organization_rki: ExtractedOrganization,
) -> Generator[ExtractedResource, None, None]:
    """Transform Biospecimen resources to extracted resources.

    Args:
        biospecimen_resources: Biospecimen resources
        extracted_platform_biospecimen: Extracted platform for Biospecimen
        extracted_platform_report_server: Extracted platform for report server
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        mex_persons: Generator for ExtractedPerson
        extracted_organization_rki: extractded organization

    Returns:
        Generator for ExtractedResource instances
    """
    identity_provider = get_provider()
    person_stable_target_id_by_email = {
        str(p.email[0]): Identifier(p.stableTargetId) for p in mex_persons
    }
    for resource in biospecimen_resources:
        if resource.anonymisiert_pseudonymisiert:
            anonymization_pseudonymization = AnonymizationPseudonymization.find(
                resource.anonymisiert_pseudonymisiert
            )
        else:
            anonymization_pseudonymization = None
        contributing_unit = (
            unit_stable_target_ids_by_synonym.get(unit_name)
            if (unit_name := resource.mitwirkende_fachabteilung)
            else None
        )
        contributor = (
            unit_stable_target_ids_by_synonym.get(unit_name)
            if (unit_name := resource.mitwirkende_personen)
            else None
        )
        external_partner = (
            unit_stable_target_ids_by_synonym.get(unit_name)
            if (unit_name := resource.externe_partner)
            else None
        )
        contact = None
        for kontakt in resource.kontakt:
            if k := person_stable_target_id_by_email.get(kontakt):
                contact = k
            elif k := unit_stable_target_ids_by_synonym.get(kontakt):
                contact = k

        source_identities = identity_provider.fetch(
            had_primary_source=extracted_platform_report_server.stableTargetId,
            identifier_in_primary_source=resource.studienbezug[0],
        )
        if source_identities:
            was_generated_by = source_identities[0].stableTargetId
        else:
            was_generated_by = None
        if resource.weiterfuehrende_dokumentation_url_oder_dateipfad:
            documentation = Link(
                language="de",
                title=resource.weiterfuehrende_dokumentation_titel,
                url=resource.weiterfuehrende_dokumentation_url_oder_dateipfad,
            )
        else:
            documentation = None
        mesh_id = [
            f"http://id.nlm.nih.gov/mesh/{id_}" for id_ in resource.id_mesh_begriff
        ]
        if resource.verwandte_publikation_doi:
            publication = resource.verwandte_publikation_doi
        else:
            publication = None
        resource_type_general = ResourceTypeGeneral["BIOSPECIMEN"]
        unit_in_charge = unit_stable_target_ids_by_synonym.get(
            resource.verantwortliche_fachabteilung
        )
        theme = [
            Theme["LABORATORY"],
            Theme["INFECTIOUS_DISEASES"],
            Theme["PHYSICAL_HEALTH"],
        ]
        yield ExtractedResource(
            accessRestriction=AccessRestriction["RESTRICTED"],
            alternativeTitle=resource.alternativer_titel,
            anonymizationPseudonymization=anonymization_pseudonymization,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            description=resource.beschreibung,
            documentation=documentation,
            externalPartner=external_partner,
            hadPrimarySource=extracted_platform_biospecimen.stableTargetId,
            identifierInPrimarySource=resource.sheet_name,
            instrumentToolOrApparatus=resource.tools_instrumente_oder_apparate,
            keyword=resource.schlagworte,
            loincId=resource.id_loinc,
            meshId=mesh_id,
            method=resource.methoden,
            methodDescription=resource.methodenbeschreibung,
            publication=publication,
            publisher=extracted_organization_rki.stableTargetId,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource.ressourcentyp_speziell,
            rights=resource.rechte,
            sizeOfDataBasis=resource.vorhandene_anzahl_der_proben,
            spatial=resource.raeumlicher_bezug,
            temporal=cast(list[TemporalEntity | str], resource.zeitlicher_bezug),
            theme=theme,
            title=resource.offizieller_titel_der_probensammlung,
            unitInCharge=unit_in_charge,
            wasGeneratedBy=was_generated_by or None,
        )
