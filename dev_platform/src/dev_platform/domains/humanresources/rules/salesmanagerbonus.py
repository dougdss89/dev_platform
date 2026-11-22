import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer


class CalculateSalesManagerBonus(BaseTransformer):

    """ Fill sales manager bonus with a multiple of the highest bonus in the same region """

    def __init__(self,
                 rules: dict[str, tuple[str, float]],
                 title_column: str = "jobtitle",
                 region_column: str = "continentname",
                 column: str = "bonus",
                ):

        # rules: {job title prefix: (region value, multiplier over the region max)}
        self._rules = rules
        self._title_column = title_column
        self._region_column = region_column
        self._column = column

    def transform(self, df: DataFrame) -> DataFrame:

        title = F.col(self._title_column)
        dtype = df.schema[self._column].dataType

        region_of_title = F.coalesce(*[
            F.when(title.like(f"{prefix}%"), F.lit(region))
            for prefix, (region, _) in self._rules.items()
        ])
        
        multiplier_of_title = F.coalesce(*[
            F.when(title.like(f"{prefix}%"), F.lit(multiplier))
            for prefix, (_, multiplier) in self._rules.items()
        ])

        region_max = df.groupBy(F.col(self._region_column).alias("_region")).agg(
            F.max(self._column).alias("_region_max")
        )

        missing = F.coalesce(F.col(self._column), F.lit(0)) == 0
        filled = (F.col("_region_max") * multiplier_of_title).cast(dtype)

        return (
            df.withColumn("_region", region_of_title)
            .join(F.broadcast(region_max), on="_region", how="left")
            .withColumn(
                self._column,
                F.when(missing, F.coalesce(filled, F.col(self._column))).otherwise(F.col(self._column)),
            )
            .drop("_region", "_region_max")
        )
