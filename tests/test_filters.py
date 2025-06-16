from collections.abc import Generator, Sequence

from mex.common.types import Identifier, TemporalEntity
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


def mocked_base_raw_data_source() -> Generator[MockedBaseRawData, None, None]:
    """Mock a BaseRawData object."""
    yield from [
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


def test_filters_skips_partners_mocked() -> None:
    """Test global filter for skipping partners."""
    source_gen = mocked_base_raw_data_source()
    sources = list(filter_by_global_rules(Identifier.generate(seed=42), source_gen))
    assert len(sources) == 3


def test_filters_skips_units_mocked() -> None:
    """Test global filter for skipping units."""
    source_gen = mocked_base_raw_data_source()
    sources = list(filter_by_global_rules(Identifier.generate(seed=42), source_gen))
    assert len(sources) == 3


def test_filters_skips_years_mocked() -> None:
    """Test global filter for skipping years before."""
    source_gen = mocked_base_raw_data_source()
    sources = list(filter_by_global_rules(Identifier.generate(seed=42), source_gen))
    assert len(sources) == 3
