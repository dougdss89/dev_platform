import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer


class CalculateSalesManagerCommission(BaseTransformer):
    
    """ Calculates the sales manager commission percentage based on their location and jobtitle """
    
    def __init__(self,
                 pct_by_title: dict[str, float],
                 title_column: str = "jobtitle",
                 column: str = "commissionpct",
                ):

        self._pct_by_title = pct_by_title
        self._title_column = title_column
        self._column = column

    def transform(self, df: DataFrame) -> DataFrame:

        title = F.col(self._title_column)
        pct_of_title = F.coalesce(*[
            F.when(title.like(f"{prefix}%"), F.lit(pct))
            for prefix, pct in self._pct_by_title.items()
        ]).cast(df.schema[self._column].dataType)

        missing = F.coalesce(F.col(self._column), F.lit(0)) == 0

        return df.withColumn(
            self._column,
            F.when(missing, F.coalesce(pct_of_title, F.col(self._column))).otherwise(F.col(self._column)),
        )
