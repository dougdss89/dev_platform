import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from dev_platform.core.common.transformer import BaseTransformer
from dev_platform.core.common.column_validator import ColumnValidator


class NormalizeGender(BaseTransformer):

    def __init__(self,
                 columns: list[str],
                 validator: ColumnValidator = ColumnValidator(StringType)):
        
        self._columns = columns
        self._validator = validator
        
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        valid_columns = self._validator.validate(df, self._columns)
        
        if not valid_columns:
            return df
        
        for col_name in valid_columns:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) == "M", "male")
                 .when(F.col(col_name) == "F", "female")
                 .otherwise(F.col(col_name))
            )

        return df