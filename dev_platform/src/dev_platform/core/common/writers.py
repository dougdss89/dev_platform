"""Writers for the dev_platform (AdventureWorks) medallion pipelines.

Target platform: Databricks + Unity Catalog. Everything is a *managed* Delta
table addressed by its 3-level name ``<catalog>.<schema>.<table>`` — no S3/ADLS
paths and no manual CREATE DATABASE ... LOCATION (Unity Catalog manages storage).

The target table is bound in the constructor, so Pipeline can call ``write(df)``.
"""

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from dev_platform.core.common.logger import get_logger


class BaseWriter(ABC):
    """One concrete subclass per write strategy. Callers depend only on this interface."""

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        raise NotImplementedError


def _normalize_layout(
    partition_by: list[str] | None,
    cluster_by: list[str] | None,
) -> tuple[list[str], list[str]]:
    """Validate the physical layout options (fail fast at construction).

    ``partitionBy`` (Hive-style directories) and ``clusterBy`` (Delta Liquid
    Clustering) are MUTUALLY EXCLUSIVE: liquid clustering is the modern Databricks
    replacement for partitioning (and ZORDER), so a table uses one or the other.
    """
    p = partition_by or []
    c = cluster_by or []
    if p and c:
        raise ValueError(
            "partition_by and cluster_by are mutually exclusive: liquid clustering (clusterBy) replaces partitioning"
        )
    return p, c


def _apply_layout(
    writer: Any,
    partition_by: list[str],
    cluster_by: list[str],
) -> Any:
    """Apply the chosen layout to a Delta DataFrameWriter.

    Both only take effect when the table is CREATED; on writes to an existing
    table Delta keeps the layout defined at creation time.
    """
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if cluster_by:
        writer = writer.clusterBy(*cluster_by)
    return writer


class TableWriter(BaseWriter):
    """Overwrite/append a managed Delta table in Unity Catalog.

    Example:
        TableWriter(spark, table="advworks_dev.trusted.product")
        TableWriter(spark, table="...", mode="append", partition_by=["date_partition"])
        TableWriter(spark, table="...", cluster_by=["customerid"])   # liquid clustering
    """

    def __init__(
        self,
        spark: SparkSession,
        table: str,
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
        cluster_by: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ):
        self._spark = spark
        self._table = table
        self._mode = mode
        self._partition_by, self._cluster_by = _normalize_layout(partition_by, cluster_by)
        self._options: dict[str, Any] = options or {}

    def write(self, df: DataFrame) -> None:
        # Default allow the schema to evolve on full reloads; Caller options win
        opts = {"overwriteSchema": "true"} if self._mode == "overwrite" else {}
        opts.update(self._options)

        writer = df.write.format("delta").mode(self._mode).options(**opts)
        writer = _apply_layout(writer, self._partition_by, self._cluster_by)
        writer.saveAsTable(self._table)


class TableMergeWriter(BaseWriter):
    """Upsert into a managed Delta table via MERGE (idempotent incremental loads).

    Creates the table on the first run (when it does not exist yet), then MERGEs
    on subsequent runs. ``merge_key`` may be a single column or a list.

    ``partition_by`` / ``cluster_by`` (mutually exclusive) define the physical
    layout and are applied only when the table is first created; later MERGEs keep
    it. Prefer ``cluster_by`` (liquid clustering) — the merge keys are a natural
    clustering choice since every MERGE probes the table on them.

    Example:
        TableMergeWriter(spark, table="advworks_dev.trusted.customer", merge_key="customerid")
        TableMergeWriter(spark, table="...", merge_key="customerid", cluster_by=["customerid"])
    """

    def __init__(
        self,
        spark: SparkSession,
        table: str,
        merge_key: str | list[str],
        partition_by: list[str] | None = None,
        cluster_by: list[str] | None = None,
        options: dict[str, Any] | None = None,
        schema_evolution: bool = False,
    ):
        self._spark = spark
        self._table = table
        self._keys = [merge_key] if isinstance(merge_key, str) else list(merge_key)
        if not self._keys:
            raise ValueError("TableMergeWriter requires at least one merge_key")
        self._partition_by, self._cluster_by = _normalize_layout(partition_by, cluster_by)
        self._options: dict[str, Any] = options or {}
        self._schema_evolution = schema_evolution

    def write(self, df: DataFrame) -> None:
        from delta.tables import DeltaTable

        if not self._spark.catalog.tableExists(self._table):
            # No target yet: seed it with a plain create (MERGE needs an existing
            # table). This is where partitioning / clustering gets defined.
            writer = _apply_layout(df.write.format("delta"), self._partition_by, self._cluster_by)

            for key, value in self._options.items():
                writer = writer.option(key, value)
            writer.saveAsTable(self._table)
            return

        condition = " AND ".join(f"t.{k} = s.{k}" for k in self._keys)
        target = DeltaTable.forName(self._spark, self._table)

        merge = target.alias("t").merge(df.alias("s"), condition)

        # Per-MERGE schema evolution. The session conf
        # spark.databricks.delta.schema.autoMerge.enabled is rejected on serverless.
        if self._schema_evolution:
            merge = merge.withSchemaEvolution()

        merge.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


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
