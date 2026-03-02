from datetime import datetime
from typing import TYPE_CHECKING

from mex.common.exceptions import MExError
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.extractors.seq_repo.model import SeqRepoSource


def filter_sources_on_latest_sequencing_date(
    seq_repo_sources: Iterable[SeqRepoSource],
) -> dict[str, SeqRepoSource]:
    """Filter sources on sequencing date, keeping only latest sequenced item.

    Args:
        seq_repo_sources: Seq Repo unfiltered extracted sources

    Returns:
        Filtered Seq Repo sources
    """
    filtered_sources = filter_by_global_rules(
        get_extracted_primary_source_id_by_name("seq-repo"), seq_repo_sources
    )
    unique_sources_with_latest_date: dict[str, SeqRepoSource] = {}
    for source in filtered_sources:
        if not source.sequencing_date:
            continue
        identifier_in_primary_source = (
            f"{source.lims_sample_id}.{source.sequencing_platform}"
        )
        if identifier_in_primary_source not in unique_sources_with_latest_date:
            unique_sources_with_latest_date[identifier_in_primary_source] = source
        else:
            item_in_dict = unique_sources_with_latest_date[identifier_in_primary_source]
            if not item_in_dict.sequencing_date:  # otherwise mypy complains
                msg = (
                    f"{identifier_in_primary_source} has no sequencing date. "
                    f"This should've been filtered out before and not happen here."
                )
                raise MExError(msg)
            item_in_dict_date = datetime.strptime(  # noqa: DTZ007
                item_in_dict.sequencing_date, "%Y-%M-%d"
            )
            source_date = datetime.strptime(source.sequencing_date, "%Y-%M-%d")  # noqa: DTZ007
            if source_date > item_in_dict_date:
                unique_sources_with_latest_date[identifier_in_primary_source] = source

    return unique_sources_with_latest_date
