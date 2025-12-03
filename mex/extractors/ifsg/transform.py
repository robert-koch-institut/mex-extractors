from mex.common.models import (
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.common.types import (
    MergedResourceIdentifier,
    Text,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_datatype import MetaDataType
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_type import MetaType
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


def transform_resource_parent_to_mex_resource(
    resource_parent: ResourceMapping,
) -> ExtractedResource:
    """Transform resource parent to mex resource.

    Args:
        resource_parent: resource parent mapping model

    Returns:
        resource parent transformed to ExtractedResource
    """
    return ExtractedResource(
        accessRestriction=resource_parent.accessRestriction[0]
        .mappingRules[0]
        .setValues,
        accrualPeriodicity=resource_parent.accrualPeriodicity[0]
        .mappingRules[0]
        .setValues,
        alternativeTitle=resource_parent.alternativeTitle[0].mappingRules[0].setValues,
        contact=get_unit_merged_id_by_synonym(
            resource_parent.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ),
        description=resource_parent.description[0].mappingRules[0].setValues,
        hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
        hasLegalBasis=resource_parent.hasLegalBasis[0].mappingRules[0].setValues,
        hasPersonalData=resource_parent.hasPersonalData[0].mappingRules[0].setValues,
        identifierInPrimarySource=resource_parent.identifierInPrimarySource[0]
        .mappingRules[0]
        .setValues,
        keyword=resource_parent.keyword[0].mappingRules[0].setValues,
        language=resource_parent.language[0].mappingRules[0].setValues,
        resourceCreationMethod=resource_parent.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues,
        resourceTypeGeneral=resource_parent.resourceTypeGeneral[0]
        .mappingRules[0]
        .setValues,
        rights=resource_parent.rights[0].mappingRules[0].setValues,
        spatial=resource_parent.spatial[0].mappingRules[0].setValues,
        theme=resource_parent.theme[0].mappingRules[0].setValues,
        title=resource_parent.title[0].mappingRules[0].setValues,
        unitInCharge=get_unit_merged_id_by_synonym(
            resource_parent.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ),
    )


def transform_resource_state_to_mex_resource(
    resource_state: ResourceMapping,
    ifsg_extracted_resource_parent: ExtractedResource,
) -> list[ExtractedResource]:
    """Transform resource state to mex resource.

    Args:
        resource_state: resource state mapping model
        ifsg_extracted_resource_parent: ExtractedResource
        meta_disease: list of MetaDisease table rows

    Returns:
        transform resource state to ExtractedResource list
    """
    bundesland_meldedaten_by_bundesland_id = {
        value.forValues[0]: value.setValues  # type: ignore[index]
        for value in resource_state.alternativeTitle[0].mappingRules
    }
    if contact_synonym := resource_state.contact[0].mappingRules[0].forValues:
        contact = get_unit_merged_id_by_synonym(contact_synonym[0])
    if not contact:
        pass
    documentation_by_bundesland_id = {
        value.forValues[0]: value.setValues  # type: ignore[index]
        for value in resource_state.documentation[0].mappingRules
    }
    keyword = resource_state.keyword[0].mappingRules[0].setValues
    spatial_by_bundesland_id = None
    if resource_state.spatial:
        spatial_by_bundesland_id = {
            value.forValues[0]: value.setValues  # type: ignore[index]
            for value in resource_state.spatial[0].mappingRules
            if value.setValues
        }
    title_by_bundesland_id = {
        value.forValues[0]: value.setValues  # type: ignore[index]
        for value in resource_state.title[0].mappingRules
        if value.setValues
    }
    if (
        unit_in_charge_synonym := resource_state.unitInCharge[0]
        .mappingRules[0]
        .forValues
    ):
        unit_in_charge = get_unit_merged_id_by_synonym(unit_in_charge_synonym[0])
    if not unit_in_charge:
        pass
    mex_resource_state: list[ExtractedResource] = []
    for (
        id_bundesland,
        bundesland_meldedaten,
    ) in bundesland_meldedaten_by_bundesland_id.items():
        spatial: list[Text] = []
        if spatial_by_bundesland_id and id_bundesland in spatial_by_bundesland_id:
            spatial = spatial_by_bundesland_id[id_bundesland]
        documentation = documentation_by_bundesland_id.get(id_bundesland, [])
        mex_resource_state.append(
            ExtractedResource(
                accessRestriction=resource_state.accessRestriction[0]
                .mappingRules[0]
                .setValues,
                accrualPeriodicity=resource_state.accrualPeriodicity[0]
                .mappingRules[0]
                .setValues,
                alternativeTitle=bundesland_meldedaten,
                contact=contact,
                documentation=documentation,
                hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
                hasLegalBasis=resource_state.hasLegalBasis[0].mappingRules[0].setValues,
                hasPersonalData=resource_state.hasPersonalData[0]
                .mappingRules[0]
                .setValues,
                identifierInPrimarySource=id_bundesland,
                isPartOf=ifsg_extracted_resource_parent.stableTargetId,
                keyword=keyword,
                language=resource_state.language[0].mappingRules[0].setValues,
                resourceCreationMethod=resource_state.resourceCreationMethod[0]
                .mappingRules[0]
                .setValues,
                resourceTypeGeneral=resource_state.resourceTypeGeneral[0]
                .mappingRules[0]
                .setValues,
                rights=resource_state.rights[0].mappingRules[0].setValues,
                spatial=spatial,
                theme=resource_state.theme[0].mappingRules[0].setValues,
                title=title_by_bundesland_id[id_bundesland],
                unitInCharge=unit_in_charge,
            )
        )
    return mex_resource_state


def get_instrument_tool_or_apparatus(
    meta_disease: MetaDisease,
    resource_disease: ResourceMapping,
) -> list[Text]:
    """Calculate instrument_tool_or_apparatus for MetaDisease reference definitions.

    Args:
        meta_disease: list of MetaDisease table rows
        resource_disease: resource disease mapping model

    Returns:
        instrument_tool_or_apparatus list
    """
    instrument_tool_or_apparatus_by_reference = {
        value.forValues[0]: value.setValues[0]  # type: ignore[index]
        for value in resource_disease.instrumentToolOrApparatus[0].mappingRules
    }
    instrument_tool_or_apparatus: list[Text] = []
    if meta_disease.reference_def_a:
        instrument_tool_or_apparatus.append(
            instrument_tool_or_apparatus_by_reference["A=1"]
        )
    if meta_disease.reference_def_b:
        instrument_tool_or_apparatus.append(
            instrument_tool_or_apparatus_by_reference["B=1"]
        )
    if meta_disease.reference_def_c:
        instrument_tool_or_apparatus.append(
            instrument_tool_or_apparatus_by_reference["C=1"]
        )
    if meta_disease.reference_def_d:
        instrument_tool_or_apparatus.append(
            instrument_tool_or_apparatus_by_reference["D=1"]
        )
    if meta_disease.reference_def_e:
        instrument_tool_or_apparatus.append(
            instrument_tool_or_apparatus_by_reference["E=1"]
        )
    return instrument_tool_or_apparatus


def transform_resource_disease_to_mex_resource(  # noqa: PLR0913
    resource_disease: ResourceMapping,
    ifsg_extracted_resource_parent: ExtractedResource,
    ifsg_extracted_resources_state: list[ExtractedResource],
    meta_disease: list[MetaDisease],
    meta_type: list[MetaType],
    id_type_of_diseases: list[int],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Transform resource disease to mex resource.

    Args:
        resource_disease: resource disease mapping model
        ifsg_extracted_resource_parent: ExtractedResource
        ifsg_extracted_resources_state: ExtractedResource
        meta_disease: list of MetaDisease table rows
        meta_type: MetaType
        id_type_of_diseases: list of disease related id_types
        extracted_organization_rki: extracted organization for RKI

    Returns:
        transform resource disease to ExtractedResource list
    """
    code_by_id_type = {m.id_type: m.code for m in meta_type}
    bundesland_by_in_bundesland = {
        value.forValues[0]: value.setValues[0]  # type: ignore[index]
        for value in resource_disease.spatial[1].mappingRules
    }
    stable_target_id_by_bundesland_id = {
        value.identifierInPrimarySource: value.stableTargetId
        for value in ifsg_extracted_resources_state
    }
    return [
        transform_resource_disease_to_mex_resource_row(
            meta_disease_row,
            resource_disease,
            ifsg_extracted_resource_parent,
            stable_target_id_by_bundesland_id,
            bundesland_by_in_bundesland,
            code_by_id_type,
            extracted_organization_rki,
        )
        for meta_disease_row in meta_disease
        if meta_disease_row.id_type in id_type_of_diseases
    ]


def transform_resource_disease_to_mex_resource_row(  # noqa: PLR0913
    meta_disease_row: MetaDisease,
    resource_disease: ResourceMapping,
    ifsg_extracted_resource_parent: ExtractedResource,
    stable_target_id_by_bundesland_id: dict[str, MergedResourceIdentifier],
    bundesland_by_in_bundesland: dict[str, Text],
    code_by_id_type: dict[int, str],
    extracted_organization_rki: ExtractedOrganization,
) -> ExtractedResource:
    """Transform resource disease row to mex resource.

    Args:
        meta_disease_row: row of ifsg meta disease
        resource_disease: resource disease mapping model
        ifsg_extracted_resource_parent: ExtractedResource
        stable_target_id_by_bundesland_id: stable target id to bundesland_id map
        bundesland_by_in_bundesland: in bundesland str to bundesland Text map
        code_by_id_type: id type to code map
        extracted_organization_rki: extracted organization for RKI

    Returns:
        transform resource disease row to ExtractedResource
    """
    name = meta_disease_row.disease_name
    icd10code = [meta_disease_row.icd10_code]
    instrument_tool_or_apparatus = get_instrument_tool_or_apparatus(
        meta_disease_row, resource_disease
    )
    is_part_of = [ifsg_extracted_resource_parent.stableTargetId]
    if meta_disease_row.in_bundesland:
        is_part_of.extend(
            stable_target_id_by_bundesland_id[bundesland_id]
            for bundesland_id in meta_disease_row.in_bundesland.split(",")
            if bundesland_id in stable_target_id_by_bundesland_id
        )

    keyword = [
        keyword
        for keyword in [
            meta_disease_row.disease_name,
            meta_disease_row.disease_name_en,
            meta_disease_row.specimen_name,
            *[rule.setValues[0] for rule in resource_disease.keyword[0].mappingRules],  # type: ignore[index]
        ]
        if keyword
    ]

    spatial: list[Text] = []
    if meta_disease_row.ifsg_bundesland == 0:
        spatial = resource_disease.spatial[0].mappingRules[0].setValues  # type: ignore[assignment]
    elif meta_disease_row.ifsg_bundesland == 1 and meta_disease_row.in_bundesland:
        for bundesland_id in meta_disease_row.in_bundesland.split(","):
            if bundesland_id in bundesland_by_in_bundesland:
                spatial.append(bundesland_by_in_bundesland[bundesland_id])

    return ExtractedResource(
        accessRestriction=resource_disease.accessRestriction[0]
        .mappingRules[0]
        .setValues,
        accrualPeriodicity=resource_disease.accrualPeriodicity[0]
        .mappingRules[0]
        .setValues,
        alternativeTitle=code_by_id_type[meta_disease_row.id_type],
        contact=get_unit_merged_id_by_synonym(
            resource_disease.contact[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ),
        hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
        hasLegalBasis=resource_disease.hasLegalBasis[0].mappingRules[0].setValues,
        hasPersonalData=resource_disease.hasPersonalData[0].mappingRules[0].setValues,
        icd10code=[i for i in icd10code if i],
        identifierInPrimarySource=(
            f"resource_disease_{meta_disease_row.id_type}_{meta_disease_row.id_schema}"
        ),
        instrumentToolOrApparatus=instrument_tool_or_apparatus,
        isPartOf=is_part_of,
        keyword=keyword,
        language=resource_disease.language[0].mappingRules[0].setValues,
        publisher=extracted_organization_rki.stableTargetId,
        resourceCreationMethod=resource_disease.resourceCreationMethod[0]
        .mappingRules[0]
        .setValues,
        resourceTypeGeneral=resource_disease.resourceTypeGeneral[0]
        .mappingRules[0]
        .setValues,
        rights=resource_disease.rights[0].mappingRules[0].setValues,
        spatial=spatial,
        theme=resource_disease.theme[0].mappingRules[0].setValues,
        title=f"Meldedaten nach Infektionsschutzgesetz (IfSG) zu {name} "
        f"(SurvNet Schema {meta_disease_row.id_schema})",
        unitInCharge=get_unit_merged_id_by_synonym(
            resource_disease.unitInCharge[0].mappingRules[0].forValues[0]  # type: ignore[index]
        ),
    )


def transform_ifsg_data_to_mex_variable_group(
    ifsg_variable_group: VariableGroupMapping,
    ifsg_extracted_resources_disease: list[ExtractedResource],
    meta_field: list[MetaField],
    id_types_of_diseases: list[int],
) -> list[ExtractedVariableGroup]:
    """Transform ifsg data to mex VariableGroup.

    Args:
        ifsg_variable_group: ifsg variable_group mapping model
        ifsg_extracted_resources_disease: ExtractedResource disease list
        meta_field: MetaField list
        id_types_of_diseases: disease related id_types

    Returns:
        transform resource parent to ExtractedResource
    """
    identifier_in_primary_source_unique_list = {
        f"{row.id_type}_{row.statement_area_group}"
        for row in meta_field
        if row.id_type in id_types_of_diseases
    }
    contained_by_by_id_type = {
        str(id_type): [
            row.stableTargetId
            for row in ifsg_extracted_resources_disease
            if str(id_type) in row.identifierInPrimarySource
        ]
        for id_type in id_types_of_diseases
    }
    label_by_statement_area_group = {
        row.forValues[0]: row.setValues[0]  # type: ignore[index]
        for row in ifsg_variable_group.label[0].mappingRules
    }
    return [
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
            identifierInPrimarySource=identifier_in_primary_source,
            containedBy=contained_by_by_id_type[
                identifier_in_primary_source.split("_")[0]
            ],
            label=label_by_statement_area_group[
                identifier_in_primary_source.split("_")[1]
            ],
        )
        for identifier_in_primary_source in identifier_in_primary_source_unique_list
    ]


def transform_ifsg_data_to_mex_variables(  # noqa: PLR0913
    filtered_variables: list[MetaField],
    ifsg_extracted_resources_disease: list[ExtractedResource],
    ifsg_extracted_variable_groups: list[ExtractedVariableGroup],
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
    meta_datatype: list[MetaDataType],
    meta_schema2field: list[MetaSchema2Field],
) -> list[ExtractedVariable]:
    """Transform ifsg data to mex Variable.

    Args:
        filtered_variables: MetaField list to transform into variables
        ifsg_extracted_resources_disease: ExtractedResource disease list
        ifsg_extracted_variable_groups: variable group default values
        meta_catalogue2item: MetaCatalogue2Item list
        meta_catalogue2item2schema: MetaCatalogue2Item2Schema list
        meta_item: MetaItem list
        meta_datatype: MetaDataType list
        meta_schema2field: mapping from id field to id schema

    Returns:
        transform filtered variable to extracted variables
    """
    data_type_by_id = {row.id_data_type: row.data_type_name for row in meta_datatype}
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group.stableTargetId
        for group in ifsg_extracted_variable_groups
    }
    resource_disease_stable_target_id_by_id = {
        row.identifierInPrimarySource: row.stableTargetId
        for row in ifsg_extracted_resources_disease
    }
    extracted_variables = []
    catalogue_id_by_id_item = {
        row.id_item: row.id_catalogue for row in meta_catalogue2item
    }
    item_by_id_item = {
        row.id_item: row for row in meta_item if row.id_item in catalogue_id_by_id_item
    }
    id_catalogue2item_list = [
        c2i.id_catalogue2item
        for c2i in meta_catalogue2item
        if c2i.id_catalogue2item
        in [c2i2s.id_catalogue2item for c2i2s in meta_catalogue2item2schema]
    ]
    id_schema_by_id_field = {row.id_field: row.id_schema for row in meta_schema2field}
    for row in filtered_variables:
        belongs_to = variable_group_by_identifier_in_primary_source.get(
            f"{row.id_type}_{row.statement_area_group}"
        )
        id_item_list = [
            c2i.id_item
            for c2i in meta_catalogue2item
            if c2i.id_catalogue == row.id_catalogue
            and c2i.id_catalogue2item in id_catalogue2item_list
        ]
        id_schema = id_schema_by_id_field[row.id_field]
        value_set = []
        for id_item in id_item_list:
            item_row = item_by_id_item[id_item]
            value_set.append(item_row.item_name)
        resource_identifier_in_primary_source = (
            f"resource_disease_{row.id_type}_{id_schema}"
        )
        if (
            resource_identifier_in_primary_source
            not in resource_disease_stable_target_id_by_id
        ):
            pass
        used_in = resource_disease_stable_target_id_by_id[
            resource_identifier_in_primary_source
        ]
        extracted_variables.append(
            ExtractedVariable(
                belongsTo=belongs_to,
                description=row.gui_tool_tip,
                dataType=data_type_by_id[row.id_data_type],
                hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
                identifierInPrimarySource=f"variable_{row.id_field}_{id_schema}",
                label=f"{row.gui_text} (berechneter Wert)",
                usedIn=used_in,
                valueSet=value_set,
            )
        )
    return extracted_variables
