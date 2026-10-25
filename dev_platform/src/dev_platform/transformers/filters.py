from pyspark.sql import DataFrame
from pyspark.sql.column import Column

from dev_platform.core.base import BaseTransformer


class FilterInvalidTransformer(BaseTransformer):
    """Remove registros que violam as regras informadas (lista de expressões Column)."""

    def __init__(self, rules: list[Column]):
        self.rules = rules

    def transform(self, df: DataFrame) -> DataFrame:
        for rule in self.rules:
            df = df.filter(rule)
        return df
