from mex.common.models import MergedConsent, MergedPerson
from mex.common.types import TemporalEntity
from mex.extractors.consent_mailer.filter import filter_persons_without_consent


def test_filter_persons_without_consent() -> None:
    persons = [
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d11H",
            email=[
                "consent_private_mail@mail-provider.abc",
                "eckelmannf@rki.de",
            ],
            fullName=["Fabian Eckelmann"],
        ),
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d22H",
            email=[
                "NOconsent_private_mail@mail-provider.abc",
            ],
            fullName=["Dont send mails"],
        ),
    ]
    contents = [
        MergedConsent(
            hasDataSubject="efVXbcoxxMAiA5IqT1d11H",
            isIndicatedAtTime=TemporalEntity("2025-07-31T10:18:35Z"),
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            identifier="efVXbcoxxMAiA5IqT1d10C",
        )
    ]
    filtered_persons = filter_persons_without_consent(persons, contents)
    assert persons[0] not in filtered_persons
    assert persons[1] in filtered_persons
