from pyspark.sql import DataFrame
from pyspark.sql.column import Column

from dev_platform.core.common.transformer import BaseTransformer


class FilterInvalidTransformer(BaseTransformer):
    """Filters out records that violate the given rules (list of Column expressions)."""

    def __init__(self, rules: list[Column]):
        self.rules = rules

    def transform(self, df: DataFrame) -> DataFrame:
        for rule in self.rules:
            df = df.filter(rule)
        return df
