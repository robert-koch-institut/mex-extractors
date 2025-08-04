from pydantic import Field

from mex.common.settings import BaseSettings
from mex.common.types import AssetsPath


class ConsentMailerSettings(BaseSettings):
    """Settings definition class for the consent mailer."""

    schedule: str = Field(
        "0 0 * * *",
        description="A valid cron string defining when to run the consent mailer",
        validation_alias="MEX_CONSENT_MAILER_SCHEDULE",
    )
    template_path: AssetsPath = Field(
        AssetsPath("mailings"),
        description=(
            "Path to the directory with the jinja template file containing the"
            " email body template, absolute path or relative to `assets_dir`."
        ),
    )
    smtp_server: str = Field(
        "localhost:1025",
        description="Address and port (<address>:<port>) of the SMTP server to use for"
        " the consent mailer.",
    )
