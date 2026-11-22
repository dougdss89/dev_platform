import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer

class ExtractPrefixLogin(BaseTransformer):
    
    def __init__(
        self,
        column: list[str, str],
        validator: ColumnValidator = ColumnValidator(StringType)
    ):
        
        self._column = column
        self._validator = validator
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        valid_columns = self._validator.validate(df, self._column)
        
        if not valid_columns:
            return df
        
        for col in valid_columns:
            df = df.withColumn(col, F.element_at(F.split(col, r"\\"), -1))
            
        return df
