from collections import defaultdict
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import yaml
from pydantic import ValidationError

from mex.common.logging import logger
from mex.extractors.assets.helpers import read_bytes

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator, Iterable, Sequence
    from os import PathLike

    from pandas._typing import Dtype

    from mex.common.models import BaseModel


PANDAS_DTYPE_MAP = defaultdict(
    lambda: "string",
    {bool: "bool", float: "Float64", int: "Int64"},
)


def get_dtypes_for_model(model: type[BaseModel]) -> dict[str, Dtype]:
    """Get the basic dtypes per field for a model from the `PANDAS_DTYPE_MAP`.

    Args:
        model: Model class for which to get pandas data types per field alias

    Returns:
        Mapping from field alias to dtype strings
    """
    return {
        f.alias or name: PANDAS_DTYPE_MAP[f.annotation or type(None)]
        for name, f in model.model_fields.items()
    }


def parse_csv[BaseModelT: BaseModel](  # noqa: C901
    path: str,
    into: type[BaseModelT],
    chunksize: int = 10000,
    summary_batch_size: int = 10000,
    **kwargs: Any,  # noqa: ANN401
) -> Generator[BaseModelT]:
    """Parse a CSV file into an iterable of the given model type.

    Args:
        path: Location of CSV file
        into: Type of model to parse
        chunksize: Buffer size for chunked reading
        summary_batch_size: Batch size for summary logs
        kwargs: Additional keywords arguments for pandas

    Returns:
        Generator for models
    """
    error_summary: defaultdict[str, int] = defaultdict(int)
    total_rows_processed = 0
    total_rows_successfully_processed = 0
    csv_bytes = read_bytes(path)
    with pd.read_csv(
        BytesIO(csv_bytes),
        chunksize=chunksize,
        dtype=get_dtypes_for_model(into),
        **kwargs,
    ) as reader:
        for i, chunk in enumerate(reader):
            logger.info(
                "parse_csv - %s chunk %s - OK",
                into.__name__,
                i,
            )
            for _, row in chunk.iterrows():
                row_dict = row.to_dict()
                for k, v in row_dict.items():
                    if pd.isna(v) or (isinstance(v, str) and not v.strip()):
                        row_dict[k] = None
                try:
                    model = into.model_validate(row_dict)
                    total_rows_successfully_processed += 1
                    yield model
                except ValidationError as error:
                    for validation_error in error.errors():
                        error_type = validation_error["type"]
                        error_summary[error_type] += 1

                total_rows_processed += 1

                if total_rows_processed % summary_batch_size == 0 and error_summary:
                    logger.error(
                        "Summarizing errors for batch with rows %s to %s",
                        total_rows_processed - summary_batch_size + 1,
                        total_rows_processed,
                    )
                    for error_type, count in error_summary.items():
                        logger.error(
                            " - Error type '%s': %s occurrences", error_type, count
                        )
                    error_summary.clear()

    if error_summary:
        logger.error("Summarizing errors for remaining rows")
        for error_type, count in error_summary.items():
            logger.error(" - Error type '%s': %s occurrences", error_type, count)
        logger.info(
            "Successfully processed %s items.", total_rows_successfully_processed
        )


def load_yaml(path: PathLike[str]) -> dict[str, Any]:
    """Load the contents of a YAML file from the given path and return as a dict."""
    with Path(path).open(encoding="utf-8") as fh:
        return cast("dict[str, Any]", yaml.safe_load(fh))


def collect_related_identifiers(
    items: Iterable[Any],
    relation_fields: Sequence[str],
) -> list[str]:
    """Collect identifiers referenced by relation fields on a collection."""
    identifiers: list[str] = []

    for item in items:
        for relation_field in relation_fields:
            related_values = getattr(item, relation_field, None)
            if related_values is None:
                continue

            if not isinstance(related_values, list):
                related_values = [related_values]

            for related_value in related_values:
                if related_value is None:
                    continue
                identifiers.append(str(related_value))

    return identifiers


def collect_related_identifier_counts(
    items: Iterable[Any],
    relation_fields: Sequence[str],
) -> dict[str, int]:
    """Collect counts of identifiers referenced by relation fields on a collection."""
    identifier_counts: dict[str, int] = {}

    for identifier in collect_related_identifiers(items, relation_fields):
        identifier_counts[identifier] = identifier_counts.get(identifier, 0) + 1

    return identifier_counts
