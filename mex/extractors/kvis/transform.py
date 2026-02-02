from mex.common.models import ExtractedResource, ExtractedVariableGroup, ResourceMapping
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
    Text,
)
from mex.extractors.kvis.models.table_models import KVISVariables
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def transform_kvis_resource_to_extracted_resource() -> ExtractedResource:
    """Transform the Resource mapping to an extracted resource. No reading from KVIS."""
    settings = Settings.get()
    mapping = ResourceMapping.model_validate(
        load_yaml(settings.kvis.mapping_path / "resource.yaml")
    )

    contact = (
        [  # TODO(mx-1662): ldap-helper
            MergedContactPointIdentifier.generate(seed=1234)
            for c in mapping.contact[0].mappingRules[0].forValues
        ]
        if mapping.contact[0].mappingRules[0].forValues
        else []
    )
    contributing_unit = (
        get_unit_merged_id_by_synonym(
            mapping.contributingUnit[0].mappingRules[0].forValues[0]
        )
        if mapping.contributingUnit[0].mappingRules[0].forValues
        else None
    )
    contributor = (
        [
            MergedPersonIdentifier.generate(seed=1234)  # TODO(mx-1662): ldap-helper
            for c in mapping.contributor[0].mappingRules[0].forValues
        ]
        if mapping.contributor[0].mappingRules[0].forValues
        else []
    )
    external_partner = (
        get_wikidata_extracted_organization_id_by_name(
            mapping.externalPartner[0].mappingRules[0].forValues[0]
        )
        if mapping.externalPartner[0].mappingRules[0].forValues
        else None
    )
    publisher = (
        get_wikidata_extracted_organization_id_by_name(
            mapping.publisher[0].mappingRules[0].forValues[0]
        )
        if mapping.publisher[0].mappingRules[0].forValues
        else None
    )
    unit_in_charge = (
        get_unit_merged_id_by_synonym(
            mapping.unitInCharge[0].mappingRules[0].forValues[0]
        )
        if mapping.unitInCharge[0].mappingRules[0].forValues
        else None
    )

    extracted_resource = ExtractedResource(
        accessRestriction=mapping.accessRestriction[0].mappingRules[0].setValues,
        accrualPeriodicity=mapping.accrualPeriodicity[0].mappingRules[0].setValues,
        alternativeTitle=mapping.alternativeTitle[0].mappingRules[0].setValues,
        contact=contact,
        contributingUnit=contributing_unit,
        contributor=contributor,
        created=mapping.created[0].mappingRules[0].setValues,
        description=mapping.description[0].mappingRules[0].setValues,
        documentation=mapping.documentation[0].mappingRules[0].setValues,
        externalPartner=external_partner,
        hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
        hasLegalBasis=mapping.hasLegalBasis[0].mappingRules[0].setValues,
        hasPurpose=mapping.hasPurpose[0].mappingRules[0].setValues,
        identifierInPrimarySource=mapping.identifierInPrimarySource[0]
        .mappingRules[0]
        .setValues,
        keyword=mapping.keyword[0].mappingRules[0].setValues,
        language=mapping.language[0].mappingRules[0].setValues,
        method=mapping.method[0].mappingRules[0].setValues,
        populationCoverage=mapping.populationCoverage[0].mappingRules[0].setValues,
        provenance=mapping.provenance[0].mappingRules[0].setValues,
        publisher=publisher,
        resourceCreationMethod=mapping.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues,
        resourceTypeGeneral=mapping.resourceTypeGeneral[0].mappingRules[0].setValues,
        resourceTypeSpecific=mapping.resourceTypeSpecific[0].mappingRules[0].setValues,
        spatial=mapping.spatial[0].mappingRules[0].setValues,
        theme=mapping.theme[0].mappingRules[0].setValues,
        title=mapping.title[0].mappingRules[0].setValues,
        unitInCharge=unit_in_charge,
    )
    load([extracted_resource])
    return extracted_resource


def transform_kvis_variables_to_extracted_variable_groups(
    kvis_extracted_resource_id: MergedResourceIdentifier,
    kvis_variables_table_entries: list[KVISVariables],
) -> list[ExtractedVariableGroup]:
    """Transform entries of the kvis variables table to extracted variable groups."""
    extracted_variable_groups: list[ExtractedVariableGroup] = []
    seen: set[str] = set()
    for item in kvis_variables_table_entries:
        if item.file_type in seen:
            continue
        seen.add(item.file_type)
        extracted_variable_groups.append(
            ExtractedVariableGroup(
                containedBy=kvis_extracted_resource_id,
                hadPrimarySource=get_extracted_primary_source_id_by_name("kvis"),
                identifierInPrimarySource=f"kvis_{item.file_type}",
                label=Text(value=item.file_type, language="de"),
            )
        )
    load(extracted_variable_groups)
    return extracted_variable_groups
