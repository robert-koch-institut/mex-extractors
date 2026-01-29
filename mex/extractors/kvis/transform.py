from mex.common.models import ExtractedResource, ResourceMapping

from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import \
    get_extracted_primary_source_id_by_name
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import \
    get_wikidata_extracted_organization_id_by_name


def transform_kvis_resource_to_extracted_resource(
    mapping: ResourceMapping
) -> ExtractedResource:
    breakpoint()
    extracted_resource = ExtractedResource(
        accessRestriction=mapping.accessRestriction[0].mappingRules[0].setValues,
        accrualPeriodicity=mapping.accrualPeriodicity[0].mappingRules[0].setValues,
        alternativeTitle=mapping.alternativeTitle[0].mappingRules[0].setValues,
        contact=mapping.contact[0].mappingRules[0].forValues, # TODO: ldap-helper
        contributingUnit=get_unit_merged_id_by_synonym(
            mapping.contributingUnit[0].mappingRules[0].forValues[0]
        ),
        contributor=[
            c # TODO: ldap-helper
            for c in mapping.contributor[0].mappingRules[0].forValues
        ],
        created=mapping.created[0].mappingRules[0].setValues,
        description=mapping.description[0].mappingRules[0].setValues,
        documentation=mapping.documentation[0].mappingRules[0].setValues,
        externalPartner=get_wikidata_extracted_organization_id_by_name(
            mapping.externalPartner[0].mappingRules[0].forValues[0]
        ),
        hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
        hasLegalBasis=mapping.hasLegalBasis[0].mappingRules[0].setValues,
        hasPurpose=mapping.hasPurpose[0].mappingRules[0].setValues,
        identifierInPrimarySource=mapping.identifierInPrimarySource[0].mappingRules[0].setValues,
        keyword=mapping.keyword[0].mappingRules[0].setValues,
        language=mapping.language[0].mappingRules[0].setValues,
        method=mapping.method[0].mappingRules[0].setValues,
        populationCoverage=mapping.populationCoverage[0].mappingRules[0].setValues,
        provenance=mapping.provenance[0].mappingRules[0].setValues,
        publisher=get_wikidata_extracted_organization_id_by_name(
            mapping.publisher[0].mappingRules[0].forValues[0]
        ),
        resourceCreationMethod=mapping.resourceCreationMethod[0].mappingRules[0].setValues,
        resourceTypeGeneral=mapping.resourceTypeGeneral[0].mappingRules[0].setValues,
        resourceTypeSpecific=mapping.resourceTypeSpecific[0].mappingRules[0].setValues,
        spatial=mapping.spatial[0].mappingRules[0].setValues,
        theme=mapping.theme[0].mappingRules[0].setValues,
        title=mapping.title[0].mappingRules[0].setValues,
        unitInCharge=get_unit_merged_id_by_synonym(
            mapping.unitInCharge[0].mappingRules[0].forValues[0]
        ),
    )
    load([extracted_resource])
    return extracted_resource