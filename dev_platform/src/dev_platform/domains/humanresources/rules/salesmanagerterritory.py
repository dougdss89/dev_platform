import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer


class FillSalesManagerTerritory(BaseTransformer):
    
    """ Fill sales manager territory id gap when they are nulls """
    
    def __init__(self, 
                territory_by_title: dict[str, int],
                title_column: str = "jobtitle",
                region_column: str = "countryregioncode",
                column: str = "territoryid",
            ):
        
        self._territory_by_title = territory_by_title
        self._column = column
        self._title_column = title_column
        self._region_column = region_column        
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        fills = [
            F.when(F.col(self._title_column).like(f"{prefix}%"), F.lit(territory_id))
            for prefix, territory_id in self._territory_by_title.items()
        ]
        
        df = df.withColumn(self._column,
                           F.when(
                               F.col(self._region_column).isNull(),
                               F.coalesce(F.col(self._column), *fills),
                           ).otherwise(F.col(self._column)), 
                        )
        
        return df