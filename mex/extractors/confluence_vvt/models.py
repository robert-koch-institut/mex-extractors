import re
from abc import abstractmethod
from collections.abc import Sequence

from mex.common.models import BaseModel
from mex.common.types import TemporalEntity
from mex.extractors.models import BaseRawData


class ConfluenceVvtCell(BaseModel):
    """Base class for cells in a confluence table."""

    def search(self, pattern: str) -> list[str]:
        """Returns found strings with matching pattern."""
        return [hit for text in self.get_texts() for hit in re.findall(pattern, text)]

    @abstractmethod
    def get_texts(self) -> list[str]:
        """Returns all texts in this cell."""
        ...


class ConfluenceVvtHeading(ConfluenceVvtCell):
    """Model class for confluence heading."""

    text: str | None

    def get_texts(self) -> list[str]:
        """Returns all text in this heading."""
        return [self.text] if self.text else []


class ConfluenceVvtValue(ConfluenceVvtCell):
    """Model class for confluence value cell."""

    texts: list[str] | None

    def get_texts(self) -> list[str]:
        """Returns all text in this value cell."""
        return self.texts if self.texts else []


class ConfluenceVvtRow(BaseModel):
    """Model class for confluence row."""

    cells: list[ConfluenceVvtHeading | ConfluenceVvtValue]

    def get_texts(self) -> list[str]:
        """Returns all text in this row."""
        return [text for cell in self.cells for text in cell.get_texts()]

    def is_heading(self) -> bool:
        """Returns whether all cells in a row are heading."""
        return all(isinstance(cell, ConfluenceVvtHeading) for cell in self.cells)


class ConfluenceVvtTable(BaseModel):
    """Model class for confluence table."""

    rows: list[ConfluenceVvtRow]

    def get_value_row_by_heading(self, heading: str) -> ConfluenceVvtRow:
        """If heading is found in a row, return the next row.

        Args:
            heading: Heading string to search for.

        Returns:
            ConfluenceVvt row instance.

        Raises:
            ValueError: If no row was found matching the given heading.
            TypeError: If next row is not ConfluenceVvt value row.
            IndexError: I there is no row after the heading we have found.
        """
        for index in range(len(self.rows)):
            row = self.rows[index]
            if not row.is_heading():
                continue
            if heading in row.get_texts():
                next_row = self.rows[index + 1]
                if next_row.is_heading():
                    msg = f"Found heading {heading}, but next row is also heading."
                    raise TypeError(msg)
                return next_row
        msg = f"No row found for heading {heading}"
        raise ValueError(msg)


class ConfluenceVvtPage(BaseRawData):
    """Model class for confluence page."""

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
        identifier_in_primary_source = sorted(
            set(
                self.tables[0].rows[0].cells[0].search(r"(\d{4}-\d{3})")
                + self.tables[0].rows[1].cells[0].search(r"(\d{4}-\d{3})")
            )
        )
        if len(identifier_in_primary_source) != 1:
            return None
        return identifier_in_primary_source[0]
