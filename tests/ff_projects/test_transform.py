from pytest import MonkeyPatch

from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
    YearMonthDay,
)
from mex.extractors.ff_projects.models.source import FFProjectsSource
from mex.extractors.ff_projects.transform import (
    get_or_create_organization,
    transform_ff_projects_source_to_extracted_activity,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


def test_transform_ff_projects_source_to_extracted_activity(
    ff_projects_activity: ActivityMapping,
) -> None:
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    organizations_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    person_id = MergedPersonIdentifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": [person_id]}
    laufzeit_bis = YearMonthDay("2019-08-31")
    laufzeit_von = YearMonthDay("2017-12-31")

    ff_projects_source = FFProjectsSource(
        foerderprogr="Funding",
        kategorie="Entgelt",
        laufzeit_bis=laufzeit_bis,
        laufzeit_cells=("2018-01-01 00:00:00", "2019-09-01 00:00:00"),
        laufzeit_von=laufzeit_von,
        lfd_nr="19",
        projektleiter="Dr Frieda Ficticious",
        rki_az="1364",
        rki_oe="FG99",
        thema_des_projekts="This is a project with a topic.",
        zuwendungs_oder_auftraggeber="Test-Institute",
    )

    extracted_activity = transform_ff_projects_source_to_extracted_activity(
        ff_projects_source,
        person_stable_target_ids_by_query_string,
        organizations_stable_target_ids_by_synonym,
        ff_projects_activity,
    )

    assert extracted_activity.model_dump(exclude_none=True, exclude_defaults=True) == {
        "activityType": ["https://mex.rki.de/item/activity-type-1"],
        "contact": [person_id],
        "end": [laufzeit_bis],
        "funderOrCommissioner": [organization_id],
        "fundingProgram": ["Funding"],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("ff-projects"),
        "identifier": Joker(),
        "identifierInPrimarySource": "19",
        "involvedPerson": [person_id],
        "responsibleUnit": ["cjna2jitPngp6yIV63cdi9"],
        "stableTargetId": Joker(),
        "start": [laufzeit_von],
        "title": [
            {"language": TextLanguage.EN, "value": "This is a project with a topic."}
        ],
    }


def test_get_or_create_organization(monkeypatch: MonkeyPatch) -> None:
    org_names = ["Existing-Institute", "New-Institute", "None"]

    existing_org_id = MergedOrganizationIdentifier.generate(seed=44)
    extracted_organizations = {"Existing-Institute": existing_org_id}

    created_orgs = []
    monkeypatch.setattr(
        "mex.extractors.ff_projects.transform.load", lambda x: created_orgs.extend(x)
    )

    result = get_or_create_organization(org_names, extracted_organizations)

    assert len(result) == 2
    assert existing_org_id in result
    result_created_orgs = [org_id for org_id in result if org_id != existing_org_id]
    assert result_created_orgs[0] == created_orgs[0].stableTargetId
