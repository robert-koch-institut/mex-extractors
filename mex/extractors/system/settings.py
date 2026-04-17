from pydantic import BaseModel, Field


class SystemSettings(BaseModel):
    """Settings for System."""

    max_run_age_in_days: int = Field(300, ge=1)
    protected_asset_prefixes: list[str] = []
