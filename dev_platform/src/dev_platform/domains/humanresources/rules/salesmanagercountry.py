import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer

class FillSalesManagerCountry(BaseTransformer):
    
    """ Fill sales manager country code gap when they are nulls """
    
    def __init__(self, 
                country_by_title: dict[str, str],
                title_column: str = "jobtitle",
                territory_column: str = "territoryid",
                column: str = "countryregioncode",
            ):
        
        self._country_by_title = country_by_title
        self._column = column
        self._title_column = title_column
        self._territory_column = territory_column        
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        fills = [
            F.when(F.col(self._title_column).like(f"{prefix}%"), F.lit(territory_id))
            for prefix, territory_id in self._country_by_title.items()
        ]
        
        df = df.withColumn(self._column,
                           F.when(
                               F.col(self._territory_column).isNotNull(),
                               F.coalesce(F.col(self._column), *fills),
                           ).otherwise(F.col(self._column)), 
                        )
        
        return df