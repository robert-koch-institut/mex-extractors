from pathlib import Path

from pandas import DataFrame, ExcelFile

from mex.common.models import ResourceMapping
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.odk.model import ODKData
from mex.extractors.settings import Settings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


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
        label_choices = get_column_dict_by_pattern(choices_sheet, "English")
        list_name_choices = choices_sheet["list_name"].to_list()
        name_choices = choices_sheet["name"].to_list()
        survey_sheet = xls.parse(
            sheet_name="survey", na_values=["", " "], keep_default_na=False
        )
        label_survey = get_column_dict_by_pattern(survey_sheet, "English")
        type_survey = survey_sheet["type"].to_list()
        name_survey = survey_sheet["name"].to_list()
        hint = get_column_dict_by_pattern(survey_sheet, "hint")
        raw_data.append(
            ODKData(
                file_name=file.name,
                hint=hint,
                label_survey=label_survey,
                label_choices=label_choices,
                list_name_choices=list_name_choices,
                name_choices=name_choices,
                name_survey=name_survey,
                type_survey=type_survey,
            )
        )
    return raw_data


def get_column_dict_by_pattern(
    sheet: DataFrame, pattern: str
) -> dict[str, list[str | float]]:
    """Get a dict of columns by matching pattern.

    Args:
        sheet: sheet to extract columns from
        pattern: pattern to match column names

    Returns:
        dictionary of matching columns by column names
    """
    return {col: sheet[col].to_list() for col in sheet.columns if pattern in col}


def get_external_partner_and_publisher_by_label(
    odk_resource_mappings: list[ResourceMapping],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract partner organization from wikidata.

    Args:
        odk_resource_mappings: list of resource mapping models

    Returns:
        Dict with organization label and WikidataOrganization
    """
    labels = {
        value
        for resource in odk_resource_mappings
        for for_values in [
            resource.publisher[0].mappingRules[0].forValues,
            resource.externalPartner[0].mappingRules[0].forValues,
        ]
        for value in for_values  # type: ignore[union-attr]
    }
    external_partner_and_publisher_by_label: dict[
        str, MergedOrganizationIdentifier
    ] = {}
    for label in labels:
        if organization_id := get_wikidata_extracted_organization_id_by_name(label):
            external_partner_and_publisher_by_label[label] = organization_id

    return external_partner_and_publisher_by_label
