import re
from abc import abstractmethod
from collections.abc import Sequence
from typing import cast

from mex.common.models import BaseModel
from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class ConfluenceVvtCell(BaseModel):
    def search(self, pattern: str) -> list[str]:
        return [hit for text in self.get_texts() for hit in re.findall(pattern, text)]

    @abstractmethod
    def get_texts(self) -> list[str]: ...


class ConfluenceVvtHeading(ConfluenceVvtCell):
    text: str | None

    def get_texts(self) -> list[str]:
        return [self.text] if self.text else []


class ConfluenceVvtValue(ConfluenceVvtCell):
    texts: list[str] | None

    def get_texts(self) -> list[str]:
        return self.texts if self.texts else []


class ConfluenceVvtRow(BaseModel):
    cells: list[ConfluenceVvtHeading | ConfluenceVvtValue]

    def get_texts(self) -> list[str]:
        return [text for cell in self.cells for text in cell.get_texts()]

    def is_heading(self) -> bool:
        return all(isinstance(cell, ConfluenceVvtHeading) for cell in self.cells)


class ConfluenceVvtTable(BaseModel):
    rows: list[ConfluenceVvtRow]

    def get_value_by_heading(self, heading: str) -> ConfluenceVvtRow:
        for index in range(len(self.rows)):
            row = self.rows[index]
            if not row.is_heading():
                continue
            if heading in row.get_texts():
                # if isinstance(self.rows[index + 1], ConfluenceVvtValue):
                return self.rows[index + 1]
                # return None
        raise ValueError(f"No row found for heading {heading}")


class ConfluenceVvtPage(BaseRawData):
    id: int
    title: str
    tables: list[ConfluenceVvtTable]

    def get_partners(self) -> Sequence[str | None]:
        """Return partners from extractor."""
        return []

    def get_start_year(self) -> TemporalEntity | None:
        """Return start year from extractor."""
        return None

    def get_end_year(self) -> TemporalEntity | None:
        """Return end year from extractor."""
        return None

    def get_units(self) -> Sequence[str | None]:
        """Return units from extractor."""
        return [None]

    def get_identifier_in_primary_source(self) -> str | None:
        """Return identifier in primary source from extractor."""
        identifier_in_primary_source = list(
            set(
                self.tables[0].rows[0].cells[0].search(r"(\d{4}-\d{3})")
                + self.tables[0].rows[1].cells[0].search(r"(\d{4}-\d{3})")
            )
        )
        if len(identifier_in_primary_source) != 1:
            return None
        return identifier_in_primary_source[0]
