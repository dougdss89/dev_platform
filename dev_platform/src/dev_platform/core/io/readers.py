from pyspark.sql import DataFrame, SparkSession

from dev_platform.core.base import BaseReader


class TableReader(BaseReader):
    """Lê qualquer tabela do Unity Catalog pelo nome completo catalog.schema.table."""

    def __init__(self, spark: SparkSession, table: str):
        self.spark = spark
        self.table = table

    def read(self) -> DataFrame:
        return self.spark.table(self.table)
