from pydantic import BaseModel


class SystemSettings(BaseModel):
    """Settings for System."""

    max_run_age_in_days: int = 30
