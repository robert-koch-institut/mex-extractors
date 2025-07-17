from collections.abc import Generator, Iterable
from typing import cast

from mex.common.logging import watch
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ResourceMapping,
)
from mex.common.types import (
    AnonymizationPseudonymization,
    Identifier,
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TemporalEntity,
)
from mex.extractors.biospecimen.models.source import BiospecimenResource


@watch()
def transform_biospecimen_resource_to_mex_resource(  # noqa: PLR0913
    biospecimen_resources: Iterable[BiospecimenResource],
    extracted_primary_source_biospecimen: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    mex_persons: Iterable[ExtractedPerson],
    extracted_organization_rki: ExtractedOrganization,
    extracted_synopse_activities: Iterable[ExtractedActivity],
    resource_mapping: ResourceMapping,
    extracted_organizations: dict[str, MergedOrganizationIdentifier],
) -> Generator[ExtractedResource, None, None]:
    """Transform Biospecimen resources to extracted resources.

    Args:
        biospecimen_resources: Biospecimen resources
        extracted_primary_source_biospecimen: Extracted platform for Biospecimen
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        mex_persons: Iterable of ExtractedPersons
        extracted_synopse_activities: extracted synopse activities
        extracted_organization_rki: extracted organization
        resource_mapping: resource mapping model with default values
        extracted_organizations: extracted organizations by label

    Returns:
        Generator for ExtractedResource instances
    """
    person_stable_target_id_by_email = {
        str(p.email[0]): Identifier(p.stableTargetId) for p in mex_persons
    }
    synopse_stable_target_id_by_studien_id = {
        activity.identifierInPrimarySource: activity.stableTargetId
        for activity in extracted_synopse_activities
    }
    access_restriction_by_zugriffsbeschraenkung = {
        rule.forValues[0]: rule.setValues
        for rule in resource_mapping.accessRestriction[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for resource in biospecimen_resources:
        if resource.anonymisiert_pseudonymisiert:
            anonymization_pseudonymization = AnonymizationPseudonymization.find(
                resource.anonymisiert_pseudonymisiert
            )
        else:
            anonymization_pseudonymization = None
        conforms_to = resource_mapping.conformsTo[0].mappingRules[0].setValues
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
            get_or_create_externe_partner(
                resource.externe_partner,
                extracted_organizations,
                extracted_primary_source_biospecimen,
            )
            if resource.externe_partner
            else []
        )
        has_personal_data = (
            resource_mapping.hasPersonalData[0].mappingRules[0].setValues
        )
        has_legal_basis = resource_mapping.hasLegalBasis[0].mappingRules[0].setValues
        language = resource_mapping.language[0].mappingRules[0].setValues

        contact: list[Identifier] = []
        for kontakt in resource.kontakt:
            if k := person_stable_target_id_by_email.get(kontakt):  # noqa: SIM114
                contact.append(k)
            elif k := unit_stable_target_ids_by_synonym.get(kontakt):
                contact.append(k)
        was_generated_by = synopse_stable_target_id_by_studien_id.get(
            resource.studienbezug[0], None
        )
        if resource.weiterfuehrende_dokumentation_url_oder_dateipfad:
            documentation = Link(
                language="de",
                title=resource.weiterfuehrende_dokumentation_titel,
                url=resource.weiterfuehrende_dokumentation_url_oder_dateipfad,
            )
        else:
            documentation = None
        loinc_id = (
            [f"https://loinc.org/{lid}" for lid in resource.id_loinc[0].split(", ")]
            if resource.id_loinc
            else []
        )
        mesh_id = [
            f"http://id.nlm.nih.gov/mesh/{id_}" for id_ in resource.id_mesh_begriff
        ]
        if resource_mapping.resourceTypeGeneral[0].mappingRules[0].forValues and (
            resource.ressourcentyp_allgemein
            in resource_mapping.resourceTypeGeneral[0].mappingRules[0].forValues
        ):
            resource_type_general = (
                resource_mapping.resourceTypeGeneral[0].mappingRules[0].setValues
            )
        else:
            resource_type_general = []
        resource_creation_method = (
            resource_mapping.resourceCreationMethod[0].mappingRules[0].setValues
        )
        unit_in_charge = unit_stable_target_ids_by_synonym.get(
            resource.verantwortliche_fachabteilung
        )
        if (
            resource_mapping.theme[0].mappingRules[1].forValues
            and resource_mapping.theme[0].mappingRules[1].forValues[0] in resource.thema
        ):
            theme = resource_mapping.theme[0].mappingRules[1].setValues
        else:
            theme = resource_mapping.theme[0].mappingRules[0].setValues
        yield ExtractedResource(
            accessRestriction=access_restriction_by_zugriffsbeschraenkung[
                resource.zugriffsbeschraenkung
            ],
            alternativeTitle=resource.alternativer_titel,
            anonymizationPseudonymization=anonymization_pseudonymization,
            conformsTo=conforms_to,
            contact=contact,
            contributingUnit=contributing_unit,
            contributor=contributor,
            description=resource.beschreibung,
            documentation=documentation,
            externalPartner=external_partner,
            hadPrimarySource=extracted_primary_source_biospecimen.stableTargetId,
            hasLegalBasis=has_legal_basis,
            hasPersonalData=has_personal_data,
            identifierInPrimarySource=f"{resource.file_name.split('.')[0]}_{resource.sheet_name}",
            instrumentToolOrApparatus=resource.tools_instrumente_oder_apparate,
            keyword=resource.schlagworte,
            language=language,
            loincId=loinc_id,
            meshId=mesh_id,
            method=resource.methoden,
            methodDescription=resource.methodenbeschreibung,
            publisher=extracted_organization_rki.stableTargetId,
            resourceCreationMethod=resource_creation_method,
            resourceTypeGeneral=resource_type_general,
            resourceTypeSpecific=resource.ressourcentyp_speziell,
            rights=resource.rechte,
            sizeOfDataBasis=resource.vorhandene_anzahl_der_proben,
            spatial=resource.raeumlicher_bezug,
            temporal=cast("list[TemporalEntity | str]", resource.zeitlicher_bezug),
            theme=theme,
            title=resource.offizieller_titel_der_probensammlung,
            unitInCharge=unit_in_charge,
            wasGeneratedBy=was_generated_by or None,
        )


def get_or_create_externe_partner(
    externe_partner: str,
    extracted_organizations: dict[str, MergedOrganizationIdentifier],
    extracted_primary_source_biospecimen: ExtractedPrimarySource,
) -> MergedOrganizationIdentifier:
    """Get extracted organization for label or create new organization.

    Args:
        externe_partner: externe partner label
        extracted_organizations: merged organization identifier extracted from wikidata
        extracted_primary_source_biospecimen: extracted primary source

    Returns:
        matched or created merged organization identifier
    """
    if externe_partner in extracted_organizations:
        return extracted_organizations[externe_partner]
    return ExtractedOrganization(
        officialName=externe_partner,
        identifierInPrimarySource=externe_partner,
        hadPrimarySource=extracted_primary_source_biospecimen.stableTargetId,
    ).stableTargetId
