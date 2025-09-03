from mex.common.models import MergedPerson
from mex.extractors.consent_mailer.transform import transform_person_to_sendable_email


def test_transform_person_to_sendable_email_only_containing_rki_mail() -> None:
    person = MergedPerson(
        identifier="efVXbcoxxMAiA5IqT1d11H",
        email=[
            "consent_private_mail@mail-provider.abc",
            "consent_working_mail@rki.de",
        ],
        fullName=["Consent Person"],
    )

    mail_msg = transform_person_to_sendable_email(person)
    assert mail_msg is not None
    assert person.email[0] not in mail_msg["To"]
    assert person.email[1] in mail_msg["To"]  # mail should be send to @rki.de address

    html_body = mail_msg.get_body("html")
    assert html_body is not None

    html_str_body = html_body.as_string()
    found_index = html_str_body.find(person.fullName[0])
    assert found_index > -1


def test_transform_person_no_rki_mail_to_none_email() -> None:
    person = MergedPerson(
        identifier="efVXbcoxxMAiA5IqT1d11H",
        email=[
            "consent_private_mail@mail-provider.abc",
            "consent_working_mail@no-rki.de",
        ],
        fullName=["Consent Person"],
    )
    mail_msg = transform_person_to_sendable_email(person)
    assert mail_msg is None
