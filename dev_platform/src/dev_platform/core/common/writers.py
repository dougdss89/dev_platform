"""Writers for the dev_platform (AdventureWorks) medallion pipelines.

Target platform: Databricks + Unity Catalog. Everything is a *managed* Delta
table addressed by its 3-level name ``<catalog>.<schema>.<table>`` — no S3/ADLS
paths and no manual CREATE DATABASE ... LOCATION (Unity Catalog manages storage).

The target table is bound in the constructor, so Pipeline can call ``write(df)``.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from dev_platform.core.common.logger import get_logger


class BaseWriter(ABC):
    """One concrete subclass per write strategy. Callers depend only on this interface."""

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        raise NotImplementedError


class TableWriter(BaseWriter):
    """Overwrite/append a managed Delta table in Unity Catalog.

    Example:
        TableWriter(spark, table="advworks_dev.trusted.product")
        TableWriter(spark, table="...", mode="append", partition_by=["date_partition"])
    """

    def __init__(
        self,
        spark: SparkSession,
        table: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ):
        self._spark = spark
        self._table = table
        self._mode = mode
        self._partition_by = partition_by or []

    def write(self, df: DataFrame) -> None:
        writer = df.write.format("delta").mode(self._mode)
        if self._partition_by:
            writer = writer.partitionBy(*self._partition_by)
        if self._mode == "overwrite":
            # Allow the schema to evolve on full reloads.
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(self._table)


class TableMergeWriter(BaseWriter):
    """Upsert into a managed Delta table via MERGE (idempotent incremental loads).

    Creates the table on the first run (when it does not exist yet), then MERGEs
    on subsequent runs. ``merge_key`` may be a single column or a list.

    Example:
        TableMergeWriter(spark, table="advworks_dev.trusted.customer", merge_key="customerid")
    """

    def __init__(self, spark: SparkSession, table: str, merge_key: str | list[str]):
        self._spark = spark
        self._table = table
        self._keys = [merge_key] if isinstance(merge_key, str) else list(merge_key)
        if not self._keys:
            raise ValueError("TableMergeWriter requires at least one merge_key")

    def write(self, df: DataFrame) -> None:
        from delta.tables import DeltaTable

        if not self._spark.catalog.tableExists(self._table):
            # No target yet: seed it with a plain create. MERGE needs an existing table.
            df.write.format("delta").saveAsTable(self._table)
            return

        condition = " AND ".join(f"t.{k} = s.{k}" for k in self._keys)
        target = DeltaTable.forName(self._spark, self._table)
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


class LoggingWriter(BaseWriter):
    """Decorator: adds logging around any BaseWriter, without changing it."""

    def __init__(self, writer: BaseWriter):
        self._writer = writer
        self._logger = get_logger(self.__class__.__name__)

    def write(self, df: DataFrame) -> None:
        name = type(self._writer).__name__
        self._logger.info("Writing via %s", name)
        self._writer.write(df)
        self._logger.info("Finished writing via %s", name)
