from mex.common.models import ExtractedPrimarySource
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.ff_projects.extract import extract_ff_projects_sources
from mex.extractors.ff_projects.filter import filter_and_log_ff_projects_sources


def test_filter_and_log_ff_projects_sources(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    unit_stable_target_ids_by_synonym = {
        "FG33": MergedOrganizationalUnitIdentifier.generate(33),
        "Department": MergedOrganizationalUnitIdentifier.generate(99),
    }
    sources = list(extract_ff_projects_sources())
    assert len(sources) == 21

    sources = list(
        filter_and_log_ff_projects_sources(
            sources,
            extracted_primary_sources["ff-projects"].stableTargetId,
            unit_stable_target_ids_by_synonym,
        )
    )
    assert len(sources) == 2

    project_topics = [s.thema_des_projekts for s in sources]
    assert project_topics == ["Fully Specified Source", "OE without Projektleiter"]
