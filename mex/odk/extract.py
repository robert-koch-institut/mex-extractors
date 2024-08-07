from pathlib import Path
from typing import Any

from pandas import DataFrame, ExcelFile

from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.odk.model import ODKData
from mex.settings import Settings


def extract_odk_raw_data() -> list[ODKData]:
    """Extract odk raw data by loading data from MS-Excel file.

    Settings:
        odk.raw_data_path: Path to the odk raw data,
                       absolute or relative to `assets_dir`

    Returns:
        list of ODK data.
    """
    settings = Settings.get()
    raw_data = []
    for file in Path(settings.odk.raw_data_path).glob("*.xlsx"):
        xls = ExcelFile(file)

        choices_sheet = xls.parse(
            sheet_name="choices", na_values=["", " "], keep_default_na=False
        )
        label_choices = get_column_dict_by_pattern(choices_sheet, "label")
        list_name = choices_sheet["list_name"].to_list()

        survey_sheet = xls.parse(
            sheet_name="survey", na_values=["", " "], keep_default_na=False
        )
        label_survey = get_column_dict_by_pattern(survey_sheet, "label")
        survey_type = survey_sheet["type"].to_list()
        name = survey_sheet["name"].to_list()
        hint = get_column_dict_by_pattern(survey_sheet, "hint")
        raw_data.append(
            ODKData(
                file_name=file.name,
                hint=hint,
                label_survey=label_survey,
                label_choices=label_choices,
                list_name=list_name,
                name=name,
                type=survey_type,
            )
        )
    return raw_data


def get_column_dict_by_pattern(
    sheet: DataFrame, pattern: str
) -> dict[str, list[str | float]]:
    """Get a dict of columns by matchting pattern.

    Args:
        sheet: sheet to extract columns from
        pattern: pattern to match column names

    Returns:
        dictionary of mathing columns by column names
    """
    return {col: sheet[col].to_list() for col in sheet.columns if pattern in col}


def get_external_partner_and_publisher_by_label(
    odk_resource_mappings: list[dict[str, Any]],
) -> dict[str, WikidataOrganization]:
    """Search and extract partner organization from wikidata.

    Args:
        odk_resource_mappings: list of resource mappings

    Returns:
        Dict with organization label and WikidataOrganization
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

    return external_partner_and_publisher_by_label
