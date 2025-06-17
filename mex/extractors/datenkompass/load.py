import json

import boto3

from mex.extractors.datenkompass.source import DatenkompassActivity
from mex.extractors.settings import Settings


def write_activity_to_json(
    datenkompassactivity: list[DatenkompassActivity],
) -> None:
    """Write activity to json."""
    naming_adapted_data = [
        {
            "Beschreibung": item.Beschreibung,
            "Datenhalter/ Beauftragung durch Behörde im Geschäftsbereich": item.Halter,
            "MEx-Identifier": item.identifier,
            "Kontakt (Herausgeber)": item.Kontakt,
            "Titel": item.Titel,
            "Schlagwort": item.Schlagwort,
            "Link oder Datenbank": item.Datenbank,
            "Formelle Voraussetzungen für den Datenerhalt": item.Voraussetzungen,
            "Hauptkategorie": item.Hauptkategorie,
            "Unterkategorie": item.Unterkategorie,
            "Rechtsgrundlage für die Zugangseröffnung": item.Rechtsgrundlage,
            "Weg des Datenerhalts": item.Weg,
            "Status (planbare Verfügbarkeit der Daten)": item.Status,
            "Datennutzungszweck": item.Datennutzungszweck,
            "Herausgeber": item.Herausgeber,
            "Kommentar": item.Kommentar,
            "Format": item.Format,
        }
        for item in datenkompassactivity
    ]

    settings = Settings.get()
    session = boto3.Session(
        aws_access_key_id=settings.s3_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.s3_secret_access_key.get_secret_value(),
    )

    s3 = session.client("s3", endpoint_url=str(settings.s3_endpoint_url))

    file_name = f"report-server_{datenkompassactivity[0].entityType}.json"
    file_content = json.dumps(naming_adapted_data, indent=2, ensure_ascii=False)

    s3.put_object(
        Bucket=settings.s3_bucket_key,
        Key=file_name,
        Body=file_content.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )
