import json
from email.headerregistry import Address
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from jinja2 import Template

from mex.common.models import MergedPerson
from mex.extractors.settings import Settings


def transform_person_to_sendable_email(person: MergedPerson) -> EmailMessage | None:
    """Transforms a person to an email requesting consent.

    Following properties are used:
    - the subject defined in 'settings.consent_mailer.template_path / "config.json"'
    - the person email addresses (ending on @rki.de) as target addresses (if none is
    present, this function will return none)
    - the template defined in 'settings.consent_mailer.template_path / "consent.html"'
    to generate the email body (text) for each person

    Args:
        person (MergedPerson): The person that needs to be transformed into a sendable
        consent email.

    Returns:
        EmailMessage | None: The email message for the defined person or None if the
        person doesn't have a @rki.de email address.
    """
    settings = Settings.get()

    rki_emails = filter(lambda mail: mail.endswith("@rki.de"), person.email)
    to_field = "; ".join(rki_emails)
    if not to_field:
        return None

    with Path(settings.consent_mailer.template_path / "config.json").open(
        encoding="utf-8"
    ) as fh:
        configs = json.load(fh)

    with Path(settings.consent_mailer.template_path / "consent.html").open(
        encoding="utf-8"
    ) as fh:
        template = Template(fh.read())

    body = _generate_email_body(person, template, configs["consent"]["template_args"])

    subject = configs["consent"]["subject"]

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = Address("Metadata Exchange", "mex", "rki.de")
    msg["To"] = to_field
    msg.add_alternative(body, subtype="html")

    return msg


def _generate_email_body(
    person: MergedPerson, template: Template, template_args: dict[str, Any]
) -> str:
    """Generates the consent email body.

    Args:
        person: person to send email to
        template: email template
        template_args: dict with args inserted into template

    Returns:
        email body string
    """
    full_name = (
        person.fullName[0]
        if person.fullName and len(person.fullName) > 0
        else "Mitarbeitende/r"
    )

    return template.render(full_name=full_name, **template_args)
