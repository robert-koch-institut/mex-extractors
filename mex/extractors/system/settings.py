from pydantic import BaseModel, Field


class SystemSettings(BaseModel):
    """Settings for System."""

    max_run_age_in_days: int = Field(30, ge=1)
