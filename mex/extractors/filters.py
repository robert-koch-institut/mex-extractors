from collections.abc import Generator, Iterable
from typing import TypeVar

from mex.common.types import Identifier
from mex.common.utils import any_contains_any
from mex.extractors.logging import log_filter
from mex.extractors.models import BaseRawData
from mex.extractors.settings import Settings

RawDataT = TypeVar("RawDataT", bound=BaseRawData)


def filter_by_global_rules(
    primary_source_id: Identifier,
    items: Iterable[RawDataT],
) -> Generator[
    RawDataT,
    None,
    None,
]:
    """Filter out items according to global filter rules, build filtered Generator.

    Args:
        primary_source_id: identifier of the primary source
        items: items, source or resource to be filtered
    """
    settings = Settings.get()
    for item in items:
        identifier_in_primary_source = item.get_identifier_in_primary_source()
        partners = item.get_partners()
        if any_contains_any(partners, settings.skip_partners):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Name [{partners}] in settings.skip_partners",
            )
            continue

        units = item.get_units()
        if any_contains_any(units, settings.skip_units):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Department [{units}] in settings.skip_units",
            )
            continue

        start_year = item.get_start_year()
        end_year = item.get_end_year()
        if (start_year and start_year.date_time.year < settings.skip_years_before) or (
            end_year and end_year.date_time.year < settings.skip_years_before
        ):
            log_filter(
                identifier_in_primary_source,
                primary_source_id,
                f"Start/End Year [{start_year}, {end_year}] "
                "before settings.skip_years_before",
            )
            continue
        yield item
