from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import DataType

from dev_platform.core.base import BaseTransformer


class CastTypesTransformer(BaseTransformer):
    """Aplica casts de tipo a colunas específicas. schema_map: coluna -> tipo Spark."""

    def __init__(self, schema_map: dict[str, DataType]):
        self.schema_map = schema_map

    def transform(self, df: DataFrame) -> DataFrame:
        for column_name, target_type in self.schema_map.items():
            df = df.withColumn(column_name, col(column_name).cast(target_type))
        return df


class CleanStringsTransformer(BaseTransformer):
    """Remove espaços e padroniza para uppercase nas colunas informadas."""

    def __init__(self, columns: list[str]):
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        for column_name in self.columns:
            df = df.withColumn(column_name, upper(trim(col(column_name))))
        return df
