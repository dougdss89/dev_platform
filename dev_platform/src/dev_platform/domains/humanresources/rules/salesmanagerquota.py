import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dev_platform.core.common.transformer import BaseTransformer

class CalculateSalesManagerQuota(BaseTransformer):
    
    """ Calculate salesmanager quota with a factor of the highest quota in the same region"""
    
    def __init__(self,
                rules: dict[str, tuple[str, float]],
                title_column: str = "jobtitle",
                region_column: str = "continentname",
                column:str = "salesquota",
            ):
        
        #rules {job title prefix: (region value, factor applied to the region max)}
        
        self._rules = rules
        self._title_column = title_column
        self._region_column = region_column
        self._column = column
        
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        title = F.col(self._title_column)
        
        region_of_title = F.coalesce(*[
            F.when(title.like(f"{prefix}%"), F.lit(region))
            for prefix, (region, _) in self._rules.items()
        ])
        
        # rules: {job title prefix: (region value, uplift over the region max)}
        uplift_of_title = F.coalesce(*[
            F.when(title.like(f"{prefix}%"), F.lit(uplift))
            for prefix, (_, uplift) in self._rules.items()
        ])

        
        region_max = df.groupby(F.col(self._region_column).alias("_region")).agg(
            F.max(self._column).alias("_region_max"))
        
        df = ((df.withColumn("_region", region_of_title)
                .join(F.broadcast(region_max),
                      on = "_region",
                      how = "left")
                
                .withColumn(
                    self._column,
                    F.coalesce(
                        F.col(self._column),
                        (F.col("_region_max") * (1 + uplift_of_title)).cast(df.schema[self._column].dataType),
                ))
                .drop("_region", "_region_max")
            ))
        
        return df