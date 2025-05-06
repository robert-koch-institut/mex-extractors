from collections.abc import Iterable

from mex.common.types import Identifier, MergedOrganizationalUnitIdentifier
from mex.common.utils import any_contains_any, contains_any
from mex.extractors.ff_projects.models.source import FFProjectsSource
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.logging import log_filter
from mex.extractors.settings import Settings


def filter_and_log_ff_projects_sources(
    sources: Iterable[FFProjectsSource],
    primary_source_id: Identifier,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[FFProjectsSource]:
    """Filter FF Projects sources and log filtered sources.

    Args:
        sources: Iterable of FFProjectSources
        primary_source_id: Identifier of primary source
        unit_stable_target_ids_by_synonym: Unit IDs grouped by synonyms

    Returns:
       List of filtered FF Projects sources
    """
    sources_filtered_by_global_rules = filter_by_global_rules(
        primary_source_id,
        sources,
    )
    return [
        source
        for source in sources_filtered_by_global_rules
        if filter_and_log_ff_projects_source(
            source, primary_source_id, unit_stable_target_ids_by_synonym
        )
    ]


def filter_and_log_ff_projects_source(  # noqa: PLR0911
    source: FFProjectsSource,
    primary_source_id: Identifier,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> bool:
    """Filter a FFprojectSource according to settings and log filtering.

    Args:
        source: FFProjectSource
        primary_source_id: Identifier of primary source
        unit_stable_target_ids_by_synonym: Unit IDs grouped by synonyms

    Settings:
        ff_projects.skip_funding: Skip sources with this funding
        ff_projects.skip_topics: Skip sources with these topics
        ff_projects.skip_years_strings: Skip sources with these years
        ff_projects.skip_clients: Skip sources with these clients

    Returns:
        False if source is filtered out, else True
    """
    settings = Settings.get()
    identifier_in_primary_source = source.lfd_nr

    if source.foerderprogr in settings.ff_projects.skip_funding:
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"Foerderprogr. [{source.foerderprogr}] in "
            f"settings.ff_projects.skip_funding",
        )
        return False

    if source.thema_des_projekts is None or contains_any(
        source.thema_des_projekts, settings.ff_projects.skip_topics
    ):
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"Thema des Projekts [{source.thema_des_projekts}] "
            "is None or in settings.ff_projects.skip_topics",
        )
        return False

    if source.rki_az is None:
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"RKI-AZ [{source.rki_az}] is None",
        )
        return False

    if any_contains_any(source.laufzeit_cells, settings.ff_projects.skip_years_strings):
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"Laufzeit von/bis [{source.laufzeit_cells}] "
            "in settings.ff_projects.skip_years_strings",
        )
        return False

    if source.zuwendungs_oder_auftraggeber and contains_any(
        source.zuwendungs_oder_auftraggeber, settings.ff_projects.skip_clients
    ):
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"Zuwendungs-/ Auftraggeber [{source.zuwendungs_oder_auftraggeber}] "
            "is None or in settings.ff_projects.skip_clients",
        )
        return False

    if source.lfd_nr is None:
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"lfd. Nr. [{source.lfd_nr}] is None",
        )
        return False

    if not source.rki_oe:
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            "RKI- OE is None",
        )
        return False

    if all(
        oe not in unit_stable_target_ids_by_synonym
        for oe in source.rki_oe.replace("/", ",").split(",")
    ):
        log_filter(
            identifier_in_primary_source,
            primary_source_id,
            f"RKI- OE [{source.rki_oe.replace('/', ',')}] are all not valid units",
        )
        return False

    return True


def filter_out_duplicate_source_ids(
    sources: Iterable[FFProjectsSource],
) -> list[FFProjectsSource]:
    """Remove duplicate `lfd_nr`s from the given sources.

    Args:
        sources: Iterable of FF Projects sources

    Returns:
        Filtered FF Projects sources
    """
    sources = list(sources)
    lfd_nrs = [source.lfd_nr for source in sources]
    return [source for source in sources if lfd_nrs.count(source.lfd_nr) == 1]
