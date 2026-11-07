from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dev_platform.core.common.transformer import BaseTransformer


class DeduplicateTransformer(BaseTransformer):
    """Collapses the DataFrame to ONE row per key.

    Needed before a MERGE: Delta fails if multiple source rows match the same
    target row (DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE). The raw
    layer keeps every landing row (e.g. one per customer-address), so the merge
    key alone is not unique until we dedup here.

    - ``keys``: column(s) that define a unique row in trusted (the merge key).
    - ``order_by``: optional column(s) used to pick WHICH duplicate to keep
      (latest wins by default). Without it, an arbitrary row is kept
      (``dropDuplicates``), which is non-deterministic across runs.
    """

    def __init__(
        self,
        keys: str | list[str],
        order_by: str | list[str] | None = None,
        descending: bool = True,
    ):
        self._keys = [keys] if isinstance(keys, str) else list(keys)
        if not self._keys:
            raise ValueError("DeduplicateTransformer requires at least one key")
        order = [] if order_by is None else ([order_by] if isinstance(order_by, str) else list(order_by))
        self._order_by = order
        self._descending = descending

    def transform(self, df: DataFrame) -> DataFrame:
        if not self._order_by:
            # No tie-breaker: keep an arbitrary row per key.
            return df.dropDuplicates(self._keys)

        direction = F.desc_nulls_last if self._descending else F.asc_nulls_last
        window = Window.partitionBy(*self._keys).orderBy(*[direction(c) for c in self._order_by])
        return (
            df.withColumn("__rn__", F.row_number().over(window))
            .filter(F.col("__rn__") == 1)
            .drop("__rn__")
        )
