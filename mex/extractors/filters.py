from collections.abc import Iterable
from typing import TypeVar

from mex.common.models import ActivityFilter
from mex.common.types import MergedPrimarySourceIdentifier
from mex.common.utils import any_contains_any
from mex.extractors.logging import log_filter
from mex.extractors.models import BaseRawData
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml

RawDataT = TypeVar("RawDataT", bound=BaseRawData)


def filter_by_global_rules(
    primary_source_id: MergedPrimarySourceIdentifier,
    items: Iterable[RawDataT],
) -> list[RawDataT,]:
    """Filter out items according to global filter rules, return filtered items.

    Args:
        primary_source_id: identifier of the primary source
        items: items, source or resource to be filtered
    """
    settings = Settings.get()
    all_activity_filter_mapping = ActivityFilter.model_validate(
        load_yaml(settings.all_filter_mapping_path / "activity_filter.yaml")
    )
    rule_by_field = {
        field.fieldInPrimarySource: field
        for field in all_activity_filter_mapping.fields
    }
    filtered_items: list[RawDataT] = []
    for item in items:
        identifier_in_primary_source = item.get_identifier_in_primary_source()
        partners = item.get_partners()
        if (
            filter_list := rule_by_field["externalAssociate"].filterRules[0].forValues
        ) and any_contains_any(partners, filter_list):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Name [{partners}] in all activity filter rules",
            )
            continue

        units = item.get_units()
        if (
            filter_list := rule_by_field["responsibleUnit"].filterRules[1].forValues
        ) and any_contains_any(units, filter_list):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Department [{units}] in all activity filter rules",
            )
            continue

        start_year = item.get_start_year()
        earliest_start_year = (
            int(year[0].split(" < ")[1])
            if (year := rule_by_field["start"].filterRules[0].forValues)
            else -1
        )
        end_year = item.get_end_year()
        earliest_end_year = (
            int(year[0].split(" < ")[1])
            if (year := rule_by_field["end"].filterRules[0].forValues)
            else -1
        )
        if (start_year and start_year.date_time.year < earliest_start_year) or (
            end_year and end_year.date_time.year < earliest_end_year
        ):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Start/End Year [{start_year}, {end_year}] "
                "before earliest year in all activity filter rules",
            )
            continue
        filtered_items.append(item)
    return filtered_items
