from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from dev_platform.core.common.transformer import BaseTransformer


class AuditTransformer(BaseTransformer):
    """Global: stamps pipeline lineage metadata - every table gets this crossing bronze"""

    def __init__(self, layer: str):
        self._layer = layer

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumns(
            {"_ingested_at": current_timestamp(), "_source_layer": lit(self._layer)}
        )
