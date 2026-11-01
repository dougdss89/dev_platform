import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType

from dev_platform.core.common.transformer import BaseTransformer


class MetadataTransformer(BaseTransformer):
    """Global: adds a metadata struct column — every table gets it crossing into a layer."""

    def __init__(self, dataset: str, ingest_type: str = "batch", source: str = "adventureworks"):
        self._dataset = dataset
        self._ingest_type = ingest_type
        self._source = source

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumns(
            {
                "__metadata__": F.struct(
                    F.lit(self._source).alias("source"),
                    F.lit(self._dataset).alias("dataset"),
                    F.lit(self._ingest_type).alias("ingest_type"),
                ),
                "date_partition": F.current_date().cast(DateType()),
            }
        )


class DropColumnsTransformer(BaseTransformer):
    """Drops the given columns (ignores any that are absent)."""

    def __init__(self, columns: list[str]):
        self._columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        present = [c for c in self._columns if c in df.columns]
        return df.drop(*present) if present else df
