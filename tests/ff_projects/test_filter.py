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
    assert len(sources) == 16

    project_topics = [s.thema_des_projekts for s in sources]
    assert project_topics == [
        "Skipped missing lfd. Nr.",
        "Skipped Kategorie",
        "Missing RKI-OE and Laufzeiten",
        "Skipped Laufzeiten",
        "Fully Specified Source",
        "Minimally Specified Source",
        "Duplicate lfd. Nr. A",
        "Duplicate lfd. Nr. B",
        "Messy Projektleiter entry",
        "Multiple Azs",
        "Messy lfd. Nr.",
        "OE without Projektleiter",
        "Projektleiter without OE",
        "Laufzeit von and bis None",
        "Skipped Laufzeit von before skipped years",
        "Skipped Laufzeit bis before skipped years",
    ]
