from collections.abc import Sequence

import pytest

from mex.common.types import MergedPrimarySourceIdentifier, TemporalEntity
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.models import BaseRawData


class MockedBaseRawData(BaseRawData):
    """Model class for testing of BaseRawData."""

    item_id: str
    partner: str
    start_year: TemporalEntity
    end_year: TemporalEntity
    unit: str
    identifier_in_primary_source: str

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return [self.partner]

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return self.start_year

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return self.end_year

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [self.unit]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        return self.identifier_in_primary_source


@pytest.fixture
def mocked_base_raw_data_source() -> list[MockedBaseRawData]:
    return [
        MockedBaseRawData(
            item_id="1",
            partner="permitted partner",
            start_year=TemporalEntity(2020),
            end_year=TemporalEntity(2021),
            unit="permitted unit",
            identifier_in_primary_source="42",
        ),
        MockedBaseRawData(
            item_id="2",
            partner=["Erika Mustermann"],
            start_year=TemporalEntity(2020),
            end_year=TemporalEntity(2021),
            unit="permitted unit",
            identifier_in_primary_source="42",
        ),
        MockedBaseRawData(
            item_id="3",
            partner="permitted partner",
            start_year=TemporalEntity(2018),
            end_year=TemporalEntity(2021),
            unit="permitted unit",
            identifier_in_primary_source="42",
        ),
        MockedBaseRawData(
            item_id="4",
            partner="permitted partner",
            start_year=TemporalEntity(2020),
            end_year=TemporalEntity(2019),
            unit="permitted unit",
            identifier_in_primary_source="42",
        ),
        MockedBaseRawData(
            item_id="5",
            partner="permitted partner",
            start_year=TemporalEntity(2020),
            end_year=TemporalEntity(2021),
            unit="FG99",
            identifier_in_primary_source="42",
        ),
    ]


def test_filters_skips_partners_mocked(
    mocked_base_raw_data_source: list[MockedBaseRawData],
) -> None:
    """Test global filter for skipping partners."""
    sources = filter_by_global_rules(
        MergedPrimarySourceIdentifier.generate(seed=42),
        mocked_base_raw_data_source,
    )
    assert len(sources) == 3


def test_filters_skips_units_mocked(
    mocked_base_raw_data_source: list[MockedBaseRawData],
) -> None:
    """Test global filter for skipping units."""
    sources = filter_by_global_rules(
        MergedPrimarySourceIdentifier.generate(seed=42),
        mocked_base_raw_data_source,
    )
    assert len(sources) == 3


def test_filters_skips_years_mocked(
    mocked_base_raw_data_source: list[MockedBaseRawData],
) -> None:
    """Test global filter for skipping years before."""
    sources = filter_by_global_rules(
        MergedPrimarySourceIdentifier.generate(seed=42),
        mocked_base_raw_data_source,
    )
    assert len(sources) == 3
