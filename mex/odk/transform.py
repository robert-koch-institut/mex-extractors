from typing import Any

from mex.common.identity import get_provider
from mex.common.models import (
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.common.wikidata.transform import (
    transform_wikidata_organizations_to_extracted_organizations,
)
from mex.odk.model import ODKData
from mex.sinks import load


def transform_odk_resources_to_mex_resources(
    odk_resource_mappings: list[dict[str, Any]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source_international_projects: ExtractedPrimarySource,
    external_partner_and_publisher_by_label: dict[str, MergedOrganizationIdentifier],
    extracted_primary_source_mex: ExtractedPrimarySource,
) -> list[ExtractedResource]:
    """Transform odk resources to mex resources.

    Args:
        odk_resource_mappings: list of resource mappings
        unit_stable_target_ids_by_synonym: dict of OrganizationalUnitIds
        extracted_primary_source_international_projects: primary source
        external_partner_and_publisher_by_label: dict of wikidata OrganizationIDs
        extracted_primary_source_mex: mex primary source

    Returns:
        list of mex resources
    """
    provider = get_provider()

    resources = []
    for resource in odk_resource_mappings:
        if rules := resource["alternativeTitle"]:
            alternative_title = rules[0]["mappingRules"][0]["setValues"]
        contributing_unit = None
        if resource.get("contributingUnit"):
            contributing_unit = [
                unit_stable_target_ids_by_synonym[synonym]
                for synonym in resource["contributingUnit"][0]["mappingRules"][0][
                    "forValues"
                ]
            ]
        description = None
        if resource.get("description"):
            description = resource["description"][0]["mappingRules"][0]["setValues"]
        method_description = None
        if resource.get("methodDescription"):
            method_description = resource["methodDescription"][0]["mappingRules"][0][
                "setValues"
            ]
        size_of_data_basis = None
        if resource.get("sizeOfDataBasis"):
            size_of_data_basis = resource["sizeOfDataBasis"][0]["mappingRules"][0][
                "setValues"
            ]

        identity = provider.fetch(
            had_primary_source=extracted_primary_source_international_projects.stableTargetId,
            identifier_in_primary_source=resource["wasGeneratedBy"][0]["mappingRules"][
                0
            ]["forValues"][0],
        )
        was_generated_by = None
        if identity:
            was_generated_by = identity[0].stableTargetId
        external_partner = [
            partner
            for name in resource["externalPartner"][0]["mappingRules"][0]["forValues"]
            if (partner := external_partner_and_publisher_by_label.get(name))
        ]
        publisher = [
            partner
            for name in resource["publisher"][0]["mappingRules"][0]["forValues"]
            if (partner := external_partner_and_publisher_by_label.get(name))
        ]
        resources.append(
            ExtractedResource(
                identifierInPrimarySource=resource["identifierInPrimarySource"][0][
                    "mappingRules"
                ][0]["setValues"],
                accessRestriction=resource["accessRestriction"][0]["mappingRules"][0][
                    "setValues"
                ],
                alternativeTitle=alternative_title,
                contact=unit_stable_target_ids_by_synonym[
                    resource["contact"][0]["mappingRules"][0]["forValues"][0]
                ],
                contributingUnit=contributing_unit,
                description=description,
                externalPartner=external_partner,
                hadPrimarySource=extracted_primary_source_mex.stableTargetId,
                keyword=resource["keyword"][0]["mappingRules"][0]["setValues"],
                language=resource["language"][0]["mappingRules"][0]["setValues"],
                meshId=resource["meshId"][0]["mappingRules"][0]["setValues"],
                method=resource["method"][0]["mappingRules"][0]["setValues"],
                methodDescription=method_description,
                publisher=publisher,
                resourceTypeGeneral=resource["resourceTypeGeneral"][0]["mappingRules"][
                    0
                ]["setValues"],
                rights=resource["rights"][0]["mappingRules"][0]["setValues"],
                sizeOfDataBasis=size_of_data_basis,
                spatial=resource["spatial"][0]["mappingRules"][0]["setValues"],
                temporal=resource["temporal"][0]["mappingRules"][0]["setValues"],
                theme=resource["theme"][0]["mappingRules"][0]["setValues"],
                title=resource["title"][0]["mappingRules"][0]["setValues"],
                unitInCharge=unit_stable_target_ids_by_synonym[
                    resource["unitInCharge"][0]["mappingRules"][0]["forValues"][0]
                ],
                wasGeneratedBy=was_generated_by,
            )
        )
    return resources


def get_external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
    extracted_primary_source_wikidata: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract wikidata OrganizationIDs for external partners and publishers.

    Args:
        odk_resource_mappings: list of resource mappings
        extracted_primary_source_wikidata: wikidata primary source

    Returns:
        dictionary of wikidata OrganizationIDs by organization label
    """
    labels = {
        value
        for resource in odk_resource_mappings
        for attribute in ["publisher", "externalPartner"]
        for value in resource[attribute][0]["mappingRules"][0]["forValues"]
    }
    external_partner_and_publisher_by_label: dict[str, WikidataOrganization] = {}
    for label in labels:
        if organization := search_organization_by_label(label):
            external_partner_and_publisher_by_label[label] = organization

    mex_extracted_organizations = list(
        transform_wikidata_organizations_to_extracted_organizations(
            external_partner_and_publisher_by_label.values(),
            extracted_primary_source_wikidata,
        )
    )
    load(mex_extracted_organizations)

    identity_provider = get_provider()
    organization_stable_target_id_by_query = {}
    for query, wikidata_org in external_partner_and_publisher_by_label.items():
        identities = identity_provider.fetch(
            had_primary_source=extracted_primary_source_wikidata.stableTargetId,
            identifier_in_primary_source=wikidata_org.identifier,
        )
        if identities:
            organization_stable_target_id_by_query[query] = (
                MergedOrganizationIdentifier(identities[0].stableTargetId)
            )

    return organization_stable_target_id_by_query


def get_variable_groups_from_raw_data(
    odk_raw_data: list[ODKData],
) -> dict[str, list[dict[str, str]]]:
    """Get variable groups from raw data by parsing for `begin_group` and `end_group`.

    Args:
        odk_raw_data: raw data extracted from Excel files

    Returns:
        dictionary of odk groups by group name
    """
    variable_groups = {}
    for file in odk_raw_data:
        in_group = False
        group: list[dict[str, str]] = []
        for row_index, type_row in enumerate(file.type):
            name = file.name[row_index]
            if not isinstance(type_row, str) or not isinstance(name, str):
                continue
            if type_row in ["note", "today"]:
                continue
            if type_row == "begin_group":
                in_group = True
                group = []
                begin_group_name = name
            elif type_row == "end_group":
                in_group = False
                variable_groups[begin_group_name] = group
            if in_group:
                row_dict = {
                    "type": type_row,
                    "name": name,
                    "file_name": file.file_name,
                }
                label_survey_dict = {
                    label_name: label[row_index]
                    for label_name, label in file.label_survey.items()
                    if isinstance(label[row_index], str)
                }
                hint_dict = {
                    hint_name: hint[row_index]
                    for hint_name, hint in file.hint.items()
                    if isinstance(hint[row_index], str)
                }
                row_dict.update(label_survey_dict)
                row_dict.update(hint_dict)
                group.append(row_dict)
    return variable_groups


def transform_odk_variable_groups_to_extracted_variable_groups(
    odk_variable_groups: dict[str, list[dict[str, str]]],
    extracted_resources_odk: list[ExtractedResource],
    extracted_primary_source_odk: ExtractedPrimarySource,
) -> list[ExtractedVariableGroup]:
    """Transform odk variable groups to mex variable groups.

    Args:
        odk_variable_groups: dictionary of odk groups by group name
        extracted_resources_odk: extracted mex resources
        extracted_primary_source_odk: odk primary source

    Returns:
        list of mex variable groups
    """
    extracted_variable_groups = []
    resource_id_by_identifier_in_primary_source = {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in extracted_resources_odk
    }
    for begin_group_name, group in odk_variable_groups.items():
        contained_by = resource_id_by_identifier_in_primary_source[
            group[0]["file_name"].split(".")[0]
        ]

        extracted_variable_groups.append(
            ExtractedVariableGroup(
                hadPrimarySource=extracted_primary_source_odk.stableTargetId,
                identifierInPrimarySource=f"begin_group-{begin_group_name}",
                containedBy=contained_by,
                label=begin_group_name,
            )
        )
    return extracted_variable_groups


def transform_odk_data_to_extracted_variables(
    extracted_resources_odk: list[ExtractedResource],
    extracted_variable_groups_odk: list[ExtractedVariableGroup],
    odk_variable_groups: dict[str, list[dict[str, str]]],
    odk_raw_data: list[ODKData],
    extracted_primary_source_odk: ExtractedPrimarySource,
) -> list[ExtractedVariable]:
    """Transform odk variables to mex variables.

    Args:
        extracted_resources_odk: extracted mex resources
        extracted_variable_groups_odk: extracted mex variable groups
        odk_variable_groups: dictionary of odk groups by group name
        odk_raw_data: raw data extracted from Excel files
        extracted_primary_source_odk: odk primary source

    Returns:
        list of mex variables
    """
    extracted_variables = []
    variable_group_identifier_in_primary_source = {
        v.identifierInPrimarySource: v.stableTargetId
        for v in extracted_variable_groups_odk
    }
    odk_raw_data_by_file_name = {file.file_name: file for file in odk_raw_data}
    resource_id_by_identifier_in_primary_source = {
        resource.identifierInPrimarySource: resource.stableTargetId
        for resource in extracted_resources_odk
    }
    for begin_group_name, group in odk_variable_groups.items():
        used_in = resource_id_by_identifier_in_primary_source[
            group[0]["file_name"].split(".")[0]
        ]

        belongs_to = variable_group_identifier_in_primary_source[
            f"begin_group-{begin_group_name}"
        ]
        for row in group:
            description = [
                cell
                for column_name, cell in row.items()
                if column_name.startswith("hint") and isinstance(cell, str)
            ]
            label = [
                cell
                for column_name, cell in row.items()
                if column_name.startswith("label") and isinstance(cell, str)
            ]
            if label == []:
                continue  # TODO: address empty label list in MX-1568
            value_set = []
            if isinstance(row["type"], str) and row["type"].startswith(
                ("select_one", "select_multiple")
            ):
                value_set = get_value_set(
                    row["type"], odk_raw_data_by_file_name[row["file_name"]]
                )

            extracted_variables.append(
                ExtractedVariable(
                    hadPrimarySource=extracted_primary_source_odk.stableTargetId,
                    identifierInPrimarySource=row["name"],
                    belongsTo=belongs_to,
                    description=description,
                    label=label,
                    usedIn=used_in,
                    valueSet=value_set,
                )
            )
    return extracted_variables


def get_value_set(type_cell: str, choice_sheet: ODKData) -> list[str]:
    """Get value sets for types cells that start with select_one or multiple_one.

    Args:
        type_cell: one type cell
        choice_sheet: choice sheet corresponding to type cell

    Returns:
        list of value sets matched to type cell
    """
    value_set_survey = (
        type_cell.removeprefix("select_one").removeprefix("select_multiple").strip()
    )
    label_choices = choice_sheet.label_choices
    list_name = choice_sheet.list_name
    value_set_choices = [value_set_survey]
    for i, list_name_row in enumerate(list_name):
        if list_name_row == value_set_survey:
            for label_column in label_choices.values():
                label_value = label_column[i]
                if isinstance(label_value, str):
                    value_set_choices.append(label_value)

    return value_set_choices
