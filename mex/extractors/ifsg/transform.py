from typing import Any

from mex.common.models import (
    ExtractedOrganization,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
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
from mex.extractors.ifsg.models.meta_type import MetaType


def transform_resource_parent_to_mex_resource(
    resource_parent: dict[str, Any],
    extracted_primary_source: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedResource:
    """Transform resource parent to mex resource.

    Args:
        resource_parent: resource_parent default values
        extracted_primary_source: ExtractedPrimarySource
        unit_stable_target_ids_by_synonym: mapping unit synonyms to
                                           MergedOrganizationalUnitIdentifier

    Returns:
        resource parent transformed to ExtractedResource
    """
    return ExtractedResource(
        accessRestriction=resource_parent["accessRestriction"][0]["mappingRules"][0][
            "setValues"
        ],
        accrualPeriodicity=resource_parent["accrualPeriodicity"][0]["mappingRules"][0][
            "setValues"
        ],
        alternativeTitle=resource_parent["alternativeTitle"][0]["mappingRules"][0][
            "setValues"
        ],
        contact=unit_stable_target_ids_by_synonym[
            resource_parent["contact"][0]["mappingRules"][0]["forValues"][0]
        ],
        description=resource_parent["description"][0]["mappingRules"][0]["setValues"],
        hadPrimarySource=extracted_primary_source.stableTargetId,
        hasLegalBasis=resource_parent["hasLegalBasis"][0]["mappingRules"][0][
            "setValues"
        ],
        hasPersonalData=resource_parent["hasPersonalData"][0]["mappingRules"][0][
            "setValues"
        ],
        identifierInPrimarySource=resource_parent["identifierInPrimarySource"][0][
            "mappingRules"
        ][0]["setValues"],
        keyword=resource_parent["keyword"][0]["mappingRules"][0]["setValues"],
        language=resource_parent["language"][0]["mappingRules"][0]["setValues"],
        resourceCreationMethod=resource_parent["resourceCreationMethod"][0][
            "mappingRules"
        ][0]["setValues"],
        resourceTypeGeneral=resource_parent["resourceTypeGeneral"][0]["mappingRules"][
            0
        ]["setValues"],
        rights=resource_parent["rights"][0]["mappingRules"][0]["setValues"],
        spatial=resource_parent["spatial"][0]["mappingRules"][0]["setValues"],
        theme=resource_parent["theme"][0]["mappingRules"][0]["setValues"],
        title=resource_parent["title"][0]["mappingRules"][0]["setValues"],
        unitInCharge=unit_stable_target_ids_by_synonym[
            resource_parent["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
        ],
    )


def transform_resource_state_to_mex_resource(
    resource_state: dict[str, Any],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_primary_source: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    meta_disease: list[MetaDisease],
) -> list[ExtractedResource]:
    """Transform resource state to mex resource.

    Args:
        resource_state: resource_state default values
        extracted_ifsg_resource_parent: ExtractedResource
        extracted_primary_source: ExtractedPrimarySource
        unit_stable_target_ids_by_synonym: mapping unit synonyms to
                                           MergedOrganizationalUnitIdentifier
        meta_disease: MetaDisease

    Returns:
        transform resource state to ExtractedResource list
    """
    bundesland_meldedaten_by_bundesland_id = {
        value["forValues"][0]: value["setValues"][0]
        for value in resource_state["alternativeTitle"][0]["mappingRules"]
    }
    documentation_by_bundesland_id = {
        value["forValues"][0]: value["setValues"][0]
        for value in resource_state["documentation"][0]["mappingRules"]
    }
    keyword = [
        keyword
        for keyword in [
            *resource_state["keyword"][0]["mappingRules"][0]["setValues"],
            *[row.disease_name for row in meta_disease if row.disease_name],
            *[row.disease_name_en for row in meta_disease if row.disease_name_en],
            *[row.specimen_name for row in meta_disease if row.specimen_name],
        ]
    ]

    spatial_by_bundesland_id = None
    if resource_state["spatial"]:
        spatial_by_bundesland_id = {
            value["forValues"][0]: value["setValues"][0]
            for value in resource_state["spatial"][0]["mappingRules"]
        }
    title_by_bundesland_id = {
        value["forValues"][0]: value["setValues"][0]
        for value in resource_state["title"][0]["mappingRules"]
    }
    mex_resource_state: list[ExtractedResource] = []
    for (
        id_bundesland,
        bundesland_meldedaten,
    ) in bundesland_meldedaten_by_bundesland_id.items():
        spatial = []
        if (
            spatial_by_bundesland_id
            and id_bundesland in spatial_by_bundesland_id.keys()
        ):
            spatial = spatial_by_bundesland_id[id_bundesland]
        documentation = (
            documentation_by_bundesland_id[id_bundesland]
            if id_bundesland in documentation_by_bundesland_id.keys()
            else []
        )
        mex_resource_state.append(
            ExtractedResource(
                accessRestriction=resource_state["accessRestriction"][0][
                    "mappingRules"
                ][0]["setValues"],
                accrualPeriodicity=resource_state["accrualPeriodicity"][0][
                    "mappingRules"
                ][0]["setValues"],
                alternativeTitle=bundesland_meldedaten,
                contact=unit_stable_target_ids_by_synonym[
                    resource_state["contact"][0]["mappingRules"][0]["forValues"][0]
                ],
                documentation=documentation,
                hadPrimarySource=extracted_primary_source.stableTargetId,
                hasLegalBasis=resource_state["hasLegalBasis"][0]["mappingRules"][0][
                    "setValues"
                ],
                hasPersonalData=resource_state["hasPersonalData"][0]["mappingRules"][0][
                    "setValues"
                ],
                identifierInPrimarySource=id_bundesland,
                isPartOf=extracted_ifsg_resource_parent.stableTargetId,
                keyword=keyword,
                language=resource_state["language"][0]["mappingRules"][0]["setValues"],
                resourceCreationMethod=resource_state["resourceCreationMethod"][0][
                    "mappingRules"
                ][0]["setValues"],
                resourceTypeGeneral=resource_state["resourceTypeGeneral"][0][
                    "mappingRules"
                ][0]["setValues"],
                rights=resource_state["rights"][0]["mappingRules"][0]["setValues"],
                spatial=spatial,
                theme=resource_state["theme"][0]["mappingRules"][0]["setValues"],
                title=title_by_bundesland_id[id_bundesland],
                unitInCharge=unit_stable_target_ids_by_synonym[
                    resource_state["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
                ],
            )
        )
    return mex_resource_state


def get_instrument_tool_or_apparatus(
    meta_disease: MetaDisease, resource_disease: dict[str, Any]
) -> list[Text]:
    """Calculate instrument_tool_or_apparatus for MetaDisease reference definitions.

    Args:
        meta_disease: MetaDisease
        resource_disease: resource_disease default values

    Returns:
        instrument_tool_or_apparatus list
    """
    instrument_tool_or_apparatus_by_reference = {
        value["forValues"][0]: value["setValues"][0]
        for value in resource_disease["instrumentToolOrApparatus"][0]["mappingRules"]
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


def transform_resource_disease_to_mex_resource(
    resource_disease: dict[str, Any],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_ifsg_resource_state: list[ExtractedResource],
    meta_disease: list[MetaDisease],
    meta_type: list[MetaType],
    id_type_of_diseases: list[int],
    extracted_primary_source: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    max_id_schema: int,
) -> list[ExtractedResource]:
    """Transform resource disease to mex resource.

    Args:
        resource_disease: resource_disease default values
        extracted_ifsg_resource_parent: ExtractedResource
        extracted_ifsg_resource_state: ExtractedResource
        meta_disease: MetaDisease
        meta_type: MetaType
        id_type_of_diseases: list of disease related id_types
        extracted_primary_source: ExtractedPrimarySource
        unit_stable_target_ids_by_synonym: mapping unit synonyms to
                                           MergedOrganizationalUnitIdentifier
        extracted_organization_rki: extracted organization for RKI
        max_id_schema: latest id schema

    Returns:
        transform resource disease to ExtractedResource list
    """
    code_by_id_type = {m.id_type: m.code for m in meta_type}
    meta_disease_row_by_id_type = {m.id_type: m for m in meta_disease}
    bundesland_by_in_bundesland = {
        value["forValues"][0]: value["setValues"][0]
        for value in resource_disease["spatial"][1]["mappingRules"]
    }
    stable_target_id_by_bundesland_id = {
        value.identifierInPrimarySource: value.stableTargetId
        for value in extracted_ifsg_resource_state
    }
    return [
        transform_resource_disease_to_mex_resource_row(
            id_type,
            resource_disease,
            extracted_ifsg_resource_parent,
            extracted_primary_source,
            stable_target_id_by_bundesland_id,
            meta_disease_row_by_id_type,
            bundesland_by_in_bundesland,
            code_by_id_type,
            unit_stable_target_ids_by_synonym,
            extracted_organization_rki,
            max_id_schema,
        )
        for id_type in id_type_of_diseases
    ]


def transform_resource_disease_to_mex_resource_row(
    id_type: int,
    resource_disease: dict[str, Any],
    extracted_ifsg_resource_parent: ExtractedResource,
    extracted_primary_source: ExtractedPrimarySource,
    stable_target_id_by_bundesland_id: dict[str, MergedResourceIdentifier],
    meta_disease_row_by_id_type: dict[int, MetaDisease],
    bundesland_by_in_bundesland: dict[str, Text],
    code_by_id_type: dict[int, str],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    max_id_schema: int,
) -> ExtractedResource:
    """Transform resource disease row to mex resource.

    Args:
        id_type: id_type of Resource row
        resource_disease: resource_disease default values
        extracted_ifsg_resource_parent: ExtractedResource
        extracted_primary_source: ExtractedPrimarySource
        stable_target_id_by_bundesland_id: stable target id ti bundesland_id map
        meta_disease_row_by_id_type: id type to meta disease row map
        bundesland_by_in_bundesland: in bundesland str to bundesland Text map
        code_by_id_type: id type to code map
        unit_stable_target_ids_by_synonym: mapping unit synonyms to
                                           MergedOrganizationalUnitIdentifier
        extracted_organization_rki: extracted organization for RKI
        max_id_schema: latest id schema

    Returns:
        transform resource disease row to ExtractedResource
    """
    meta_disease_row = meta_disease_row_by_id_type[id_type]
    name = meta_disease_row.disease_name
    icd10code = [meta_disease_row_by_id_type[id_type].icd10_code]
    instrument_tool_or_apparatus = get_instrument_tool_or_apparatus(
        meta_disease_row, resource_disease
    )
    is_part_of = [extracted_ifsg_resource_parent.stableTargetId]
    if meta_disease_row.in_bundesland:
        is_part_of.extend(
            stable_target_id_by_bundesland_id[bundesland_id]
            for bundesland_id in meta_disease_row.in_bundesland.split(",")
            if bundesland_id in stable_target_id_by_bundesland_id.keys()
        )

    keyword = [
        keyword
        for keyword in [
            meta_disease_row.disease_name,
            meta_disease_row.disease_name_en,
            meta_disease_row.specimen_name,
            *[
                rule["setValues"][0]
                for rule in resource_disease["keyword"][0]["mappingRules"]
            ],
        ]
        if keyword
    ]

    spatial = []
    if meta_disease_row.ifsg_bundesland == 0:
        spatial = resource_disease["spatial"][0]["mappingRules"][0]["setValues"]
    elif meta_disease_row.ifsg_bundesland == 1 and meta_disease_row.in_bundesland:
        for bundesland_id in meta_disease_row.in_bundesland.split(","):
            if bundesland_id in bundesland_by_in_bundesland.keys():
                spatial.append(bundesland_by_in_bundesland[bundesland_id])

    return ExtractedResource(
        accessRestriction=resource_disease["accessRestriction"][0]["mappingRules"][0][
            "setValues"
        ],
        accrualPeriodicity=resource_disease["accrualPeriodicity"][0]["mappingRules"][0][
            "setValues"
        ],
        alternativeTitle=code_by_id_type[id_type],
        contact=unit_stable_target_ids_by_synonym[
            resource_disease["contact"][0]["mappingRules"][0]["forValues"][0]
        ],
        hadPrimarySource=extracted_primary_source.stableTargetId,
        hasLegalBasis=resource_disease["hasLegalBasis"][0]["mappingRules"][0][
            "setValues"
        ],
        hasPersonalData=resource_disease["hasPersonalData"][0]["mappingRules"][0][
            "setValues"
        ],
        icd10code=[i for i in icd10code if i],
        identifierInPrimarySource=f"Meta.Disease_{id_type}_{max_id_schema}",
        instrumentToolOrApparatus=instrument_tool_or_apparatus,
        isPartOf=is_part_of,
        keyword=keyword,
        language=resource_disease["language"][0]["mappingRules"][0]["setValues"],
        publisher=extracted_organization_rki.stableTargetId,
        resourceCreationMethod=resource_disease["resourceCreationMethod"][0][
            "mappingRules"
        ][0]["setValues"],
        resourceTypeGeneral=resource_disease["resourceTypeGeneral"][0]["mappingRules"][
            0
        ]["setValues"],
        rights=resource_disease["rights"][0]["mappingRules"][0]["setValues"],
        spatial=spatial,
        theme=resource_disease["theme"][0]["mappingRules"][0]["setValues"],
        title=f"Meldedaten nach Infektionsschutzgesetz (IfSG) zu {name}",
        unitInCharge=unit_stable_target_ids_by_synonym[
            resource_disease["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
        ],
    )


def transform_ifsg_data_to_mex_variable_group(
    ifsg_variable_group: dict[str, Any],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_primary_source: ExtractedPrimarySource,
    meta_field: list[MetaField],
    id_types_of_diseases: list[int],
    max_id_schema: int,
) -> list[ExtractedVariableGroup]:
    """Transform ifsg data to mex VariableGroup.

    Args:
        ifsg_variable_group: ifsg_variable_group default values
        extracted_ifsg_resource_disease: ExtractedResource disease list
        extracted_primary_source: ExtractedPrimarySource
        meta_field: MetaField list
        id_types_of_diseases: disease related id_types
        max_id_schema: latest id schema

    Returns:
        transform resource parent to ExtractedResource
    """
    identifier_in_primary_source_unique_list = set(
        [
            f"{row.id_type}_{row.statement_area_group}"
            for row in meta_field
            if row.id_type in id_types_of_diseases
        ]
    )
    extracted_ifsg_resource_disease_stable_target_id_by_id_type = {
        str(row.identifierInPrimarySource): row.stableTargetId
        for row in extracted_ifsg_resource_disease
    }
    label_by_statement_area_group = {
        row["forValues"][0]: row["setValues"][0]
        for row in ifsg_variable_group["label"][0]["mappingRules"]
    }
    return [
        ExtractedVariableGroup(
            hadPrimarySource=extracted_primary_source.stableTargetId,
            identifierInPrimarySource=identifier_in_primary_source,
            containedBy=extracted_ifsg_resource_disease_stable_target_id_by_id_type[
                f'Meta.Disease_{identifier_in_primary_source.split("_")[0]}_{max_id_schema}'
            ],
            label=label_by_statement_area_group[
                identifier_in_primary_source.split("_")[1]
            ],
        )
        for identifier_in_primary_source in identifier_in_primary_source_unique_list
    ]


def transform_ifsg_data_to_mex_variables(
    filtered_variables: list[MetaField],
    extracted_ifsg_resource_disease: list[ExtractedResource],
    extracted_ifsg_variable_group: list[ExtractedVariableGroup],
    extracted_primary_sources_ifsg: ExtractedPrimarySource,
    meta_catalogue2item: list[MetaCatalogue2Item],
    meta_catalogue2item2schema: list[MetaCatalogue2Item2Schema],
    meta_item: list[MetaItem],
    meta_datatype: list[MetaDataType],
    max_id_schema: int,
) -> list[ExtractedVariable]:
    """Transform ifsg data to mex Variable.

    Args:
        filtered_variables: MetaField list to transform into variables
        extracted_ifsg_resource_disease: ExtractedResource disease list
        extracted_ifsg_variable_group: variable group default values
        extracted_primary_sources_ifsg: ExtractedPrimarySource
        meta_catalogue2item: MetaCatalogue2Item list
        meta_catalogue2item2schema: MetaCatalogue2Item2Schema list
        meta_item: MetaItem list
        meta_datatype: MetaDataType list
        max_id_schema: latest id schema

    Returns:
        transform filtered variable to extracted variables
    """
    data_type_by_id = {row.id_data_type: row.data_type_name for row in meta_datatype}
    variable_group_by_identifier_in_primary_source = {
        group.identifierInPrimarySource: group.stableTargetId
        for group in extracted_ifsg_variable_group
    }
    resource_disease_stable_target_id_by_id_type = {
        row.identifierInPrimarySource.split("_")[1]: row.stableTargetId
        for row in extracted_ifsg_resource_disease
    }
    extracted_variables = []
    catalogue_id_by_id_item = {
        row.id_item: row.id_catalogue for row in meta_catalogue2item
    }
    item_by_id_item = {
        row.id_item: row
        for row in meta_item
        if row.id_item in catalogue_id_by_id_item.keys()
    }
    id_catalogue2item_list = [
        c2i.id_catalogue2item
        for c2i in meta_catalogue2item
        if c2i.id_catalogue2item
        in [c2i2s.id_catalogue2item for c2i2s in meta_catalogue2item2schema]
    ]
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
        value_set = []
        for id_item in id_item_list:
            item_row = item_by_id_item[id_item]
            value_set.append(item_row.item_name)

        used_in = resource_disease_stable_target_id_by_id_type[str(row.id_type)]
        extracted_variables.append(
            ExtractedVariable(
                belongsTo=belongs_to,
                description=row.gui_tool_tip,
                dataType=data_type_by_id[row.id_data_type],
                hadPrimarySource=extracted_primary_sources_ifsg.stableTargetId,
                identifierInPrimarySource=f"Meta.Field_{row.id_field}_{max_id_schema}",
                label=f"{row.gui_text} (berechneter Wert)",
                usedIn=used_in,
                valueSet=value_set,
            )
        )
    return extracted_variables
