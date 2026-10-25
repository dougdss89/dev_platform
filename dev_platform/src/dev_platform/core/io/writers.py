from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from dev_platform.core.base import BaseWriter


class TableWriter(BaseWriter):
    """Escreve um DataFrame como tabela do Unity Catalog (append/overwrite)."""

    def __init__(self, table: str, mode: str = "append"):
        self.table = table
        self.mode = mode

    def write(self, df: DataFrame) -> None:
        df.write.mode(self.mode).saveAsTable(self.table)


class TableMergeWriter(BaseWriter):
    """Upsert (merge) por merge_key — evita duplicatas no reprocessamento."""

    def __init__(self, spark: SparkSession, table: str, merge_key: str):
        self.spark = spark
        self.table = table
        self.merge_key = merge_key

    def write(self, df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.table):
            df.write.mode("overwrite").saveAsTable(self.table)
            return

        delta_table = DeltaTable.forName(self.spark, self.table)
        key = self.merge_key
        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{key} = source.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
