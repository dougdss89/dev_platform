import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer


class FillSalesManagerRegion(BaseTransformer):
    
    """ Fill sales manager Region gap when they are nulls """
    
    def __init__(self, 
                territory_by_title: dict[str, str],
                title_column: str = "jobtitle",
                continent_column: str = "continentname",
                column: str = "regionname",
            ):
        
        self._territory_by_title = territory_by_title
        self._column = column
        self._title_column = title_column
        self._continent_column = continent_column        
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        fills = [
            F.when(F.col(self._title_column).like(f"{prefix}%"), F.lit(territory_id))
            for prefix, territory_id in self._territory_by_title.items()
        ]
        
        df = df.withColumn(self._column,
                           F.when(
                               F.col(self._continent_column).isNull(),
                               F.coalesce(F.col(self._column), *fills),
                           ).otherwise(F.col(self._column)), 
                        )
        
        return df