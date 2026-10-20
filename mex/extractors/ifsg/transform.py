from typing import TYPE_CHECKING

from mex.common.models import (
    ExtractedOrganization,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
    VariableGroupMapping,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
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
        accessRestriction=resource_parent.accessRestriction.get_static_values(),
        accrualPeriodicity=resource_parent.accrualPeriodicity.get_static_values(),
        alternativeTitle=resource_parent.alternativeTitle.get_static_values(),
        contact=get_unit_merged_id_by_synonym(
            resource_parent.contact.get_mappable_values()[0]
        ),
        description=resource_parent.description.get_static_values(),
        hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
        hasLegalBasis=resource_parent.hasLegalBasis.get_static_values(),
        hasPersonalData=resource_parent.hasPersonalData.get_static_values(),
        identifierInPrimarySource=resource_parent.identifierInPrimarySource.get_static_values(),
        keyword=resource_parent.keyword.get_static_values(),
        language=resource_parent.language.get_static_values(),
        resourceCreationMethod=resource_parent.resourceCreationMethod.get_static_values(),
        resourceTypeGeneral=resource_parent.resourceTypeGeneral.get_static_values(),
        rights=resource_parent.rights.get_static_values(),
        spatial=resource_parent.spatial.get_static_values(),
        theme=resource_parent.theme.get_static_values(),
        title=resource_parent.title.get_static_values(),
        unitInCharge=get_unit_merged_id_by_synonym(
            resource_parent.unitInCharge.get_mappable_values()[0]
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
    if contact_synonym := resource_state.contact.get_mappable_values():
        contact = get_unit_merged_id_by_synonym(contact_synonym[0])
    keyword = resource_state.keyword.get_static_values()
    if unit_in_charge_synonym := resource_state.unitInCharge.get_mappable_values():
        unit_in_charge = get_unit_merged_id_by_synonym(unit_in_charge_synonym[0])
    return [
        ExtractedResource(
            accessRestriction=resource_state.accessRestriction.get_static_values(),
            accrualPeriodicity=resource_state.accrualPeriodicity.get_static_values(),
            alternativeTitle=resource_state.alternativeTitle.get_mapped_values(
                id_bundesland
            ),
            contact=contact,
            description=resource_state.description.get_static_values(),
            documentation=resource_state.documentation.get_mapped_values(id_bundesland),
            hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
            hasLegalBasis=resource_state.hasLegalBasis.get_static_values(),
            hasPersonalData=resource_state.hasPersonalData.get_static_values(),
            identifierInPrimarySource=id_bundesland,
            keyword=keyword,
            language=resource_state.language.get_static_values(),
            relatedResource=ifsg_extracted_resource_parent.stableTargetId,
            resourceCreationMethod=resource_state.resourceCreationMethod.get_static_values(),
            resourceTypeGeneral=resource_state.resourceTypeGeneral.get_static_values(),
            rights=resource_state.rights.get_static_values(),
            spatial=resource_state.spatial.get_mapped_values(id_bundesland),
            theme=resource_state.theme.get_static_values(),
            title=resource_state.title.get_mapped_values(id_bundesland),
            unitInCharge=unit_in_charge,
        )
        for id_bundesland in dict.fromkeys(
            resource_state.alternativeTitle.get_mappable_values()
        )
    ]


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
    mapping = resource_disease.instrumentToolOrApparatus
    instrument_tool_or_apparatus: list[Text] = []
    if meta_disease.reference_def_a:
        instrument_tool_or_apparatus.extend(mapping.get_mapped_values("A=1"))
    if meta_disease.reference_def_b:
        instrument_tool_or_apparatus.extend(mapping.get_mapped_values("B=1"))
    if meta_disease.reference_def_c:
        instrument_tool_or_apparatus.extend(mapping.get_mapped_values("C=1"))
    if meta_disease.reference_def_d:
        instrument_tool_or_apparatus.extend(mapping.get_mapped_values("D=1"))
    if meta_disease.reference_def_e:
        instrument_tool_or_apparatus.extend(mapping.get_mapped_values("E=1"))
    return instrument_tool_or_apparatus


def transform_resource_disease_to_mex_resource(  # noqa: PLR0913, PLR0917
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
            code_by_id_type,
            extracted_organization_rki,
        )
        for meta_disease_row in meta_disease
        if meta_disease_row.id_type in id_type_of_diseases
    ]


def transform_resource_disease_to_mex_resource_row(  # noqa: PLR0913, PLR0917
    meta_disease_row: MetaDisease,
    resource_disease: ResourceMapping,
    ifsg_extracted_resource_parent: ExtractedResource,
    stable_target_id_by_bundesland_id: dict[str, MergedResourceIdentifier],
    code_by_id_type: dict[int, str],
    extracted_organization_rki: ExtractedOrganization,
) -> ExtractedResource:
    """Transform resource disease row to mex resource.

    Args:
        meta_disease_row: row of ifsg meta disease
        resource_disease: resource disease mapping model
        ifsg_extracted_resource_parent: ExtractedResource
        stable_target_id_by_bundesland_id: stable target id to bundesland_id map
        code_by_id_type: id type to code map
        extracted_organization_rki: extracted organization for RKI

    Returns:
        transform resource disease row to ExtractedResource
    """
    name = meta_disease_row.disease_name
    has_code_values = [meta_disease_row.icd10_code]
    instrument_tool_or_apparatus = get_instrument_tool_or_apparatus(
        meta_disease_row, resource_disease
    )
    related_resource = [ifsg_extracted_resource_parent.stableTargetId]
    if meta_disease_row.in_bundesland:
        related_resource.extend(
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
        spatial = resource_disease.spatial.get_static_values()  # type: ignore[assignment]
    elif meta_disease_row.ifsg_bundesland == 1 and meta_disease_row.in_bundesland:
        for bundesland_id in meta_disease_row.in_bundesland.split(","):
            spatial.extend(resource_disease.spatial.get_mapped_values(bundesland_id))

    return ExtractedResource(
        accessRestriction=resource_disease.accessRestriction.get_static_values(),
        accrualPeriodicity=resource_disease.accrualPeriodicity.get_static_values(),
        alternativeTitle=code_by_id_type[meta_disease_row.id_type],
        contact=get_unit_merged_id_by_synonym(
            resource_disease.contact.get_mappable_values()[0]
        ),
        description=resource_disease.description.get_static_values(),
        hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
        hasLegalBasis=resource_disease.hasLegalBasis.get_static_values(),
        hasPersonalData=resource_disease.hasPersonalData.get_static_values(),
        hasCodeValues=[hcv for hcv in has_code_values if hcv],
        hasCodingSystem=resource_disease.hasCodingSystem.get_static_values(),
        identifierInPrimarySource=(
            f"resource_disease_{meta_disease_row.id_type}_{meta_disease_row.id_schema}"
        ),
        instrumentToolOrApparatus=instrument_tool_or_apparatus,
        keyword=keyword,
        language=resource_disease.language.get_static_values(),
        publisher=extracted_organization_rki.stableTargetId,
        relatedResource=related_resource,
        resourceCreationMethod=resource_disease.resourceCreationMethod.get_static_values(),
        resourceTypeGeneral=resource_disease.resourceTypeGeneral.get_static_values(),
        rights=resource_disease.rights.get_static_values(),
        spatial=spatial,
        theme=resource_disease.theme.get_static_values(),
        title=f"Meldedaten nach Infektionsschutzgesetz (IfSG) zu {name} "
        f"(SurvNet Schema {meta_disease_row.id_schema})",
        unitInCharge=get_unit_merged_id_by_synonym(
            resource_disease.unitInCharge.get_mappable_values()[0]
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
        f"{row.id_type}_{row.statement_area_group}": None
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
    return [
        ExtractedVariableGroup(
            hadPrimarySource=get_extracted_primary_source_id_by_name("ifsg"),
            identifierInPrimarySource=identifier_in_primary_source,
            containedBy=contained_by_by_id_type[
                identifier_in_primary_source.split("_")[0]
            ],
            label=ifsg_variable_group.label.get_mapped_values(
                identifier_in_primary_source.split("_")[1]
            ),
        )
        for identifier_in_primary_source in identifier_in_primary_source_unique_list
    ]


def transform_ifsg_data_to_mex_variables(  # noqa: PLR0913, PLR0917
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
            continue
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
