from mex.common.models import MergedConsent, MergedPerson
from mex.extractors.publisher.filter import filter_persons_with_consent


def test_filter_persons_with_consent(
    merged_person_list: list[MergedPerson],
    merged_consent_list: list[MergedConsent],
) -> None:
    result = filter_persons_with_consent(merged_person_list, merged_consent_list)
    assert len(result) == 1
    assert result[0].model_dump(exclude_defaults=True, mode="json") == {
        "identifier": "PersonPositiveConsent",
        "fullName": ["Consent, Positive"],
        "memberOf": ["SomeUnitIdentifier"],
    }
