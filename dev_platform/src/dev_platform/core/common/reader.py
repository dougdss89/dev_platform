"""Readers for the dev_platform (AdventureWorks) medallion pipelines.

Target platform: Databricks + Unity Catalog. Sources are either:
  * parquet files landed in a UC Volume (raw ingestion), or
  * a UC table (e.g. trusted reading from raw).

The source (table name / volume path) is bound in the constructor, so the
Pipeline can call ``read()`` with no arguments — matching BaseWriter.write(df)
and BaseTransformer.transform(df).
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from dev_platform.core.common.logger import get_logger


class BaseReader(ABC):
    """One concrete subclass per source. Callers depend only on this interface."""

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplementedError


class TableReader(BaseReader):
    """Reads a Unity Catalog table by its fully-qualified name.

    Example:
        TableReader(spark, table="advworks_dev.raw.customer")
    """

    def __init__(self, spark: SparkSession, table: str):
        self._spark = spark
        self._table = table

    def read(self) -> DataFrame:
        return self._spark.read.table(self._table)


class VolumeParquetReader(BaseReader):
    """Reads parquet landed under a Unity Catalog Volume.

    Example (landing layout /Volumes/<catalog>/<schema>/<volume>/<dataset>):
        VolumeParquetReader.for_landing(spark, catalog="example", dataset="customer")
        VolumeParquetReader(spark, path="/Volumes/example/raw/landing_data/customer")
    """

    def __init__(self, spark: SparkSession, path: str):
        self._spark = spark
        self._path = path

    @classmethod
    def for_landing(
        cls,
        spark: SparkSession,
        catalog: str,
        dataset: str,
        schema: str = "raw",
        volume: str = "landing_data",
    ) -> "VolumeParquetReader":
        """Builds the reader for the project's landing layout."""
        return cls(spark, path=f"/Volumes/{catalog}/{schema}/{volume}/{dataset}")

    def read(self) -> DataFrame:
        return self._spark.read.parquet(self._path)


class LoggingReader(BaseReader):
    """Decorator: adds logging around any BaseReader, without changing it."""

    def __init__(self, reader: BaseReader):
        self._reader = reader
        self._logger = get_logger(self.__class__.__name__)

    def read(self) -> DataFrame:
        name = type(self._reader).__name__
        self._logger.info("Reading via %s", name)
        df = self._reader.read()
        self._logger.info("Finished reading via %s", name)
        return df
