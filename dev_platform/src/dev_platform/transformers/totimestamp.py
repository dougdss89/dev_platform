
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, TimestampType
from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer

class NormalizeToTimestamp(BaseTransformer):
    """
    Convert epoch milliseconds to timestamp
    """
    
    def __init__(self, 
                columns:list[str], 
                validator: ColumnValidator = ColumnValidator(LongType),
            ):
        
        self._columns = columns
        self._validator = validator
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        valid_columns = self._validator.validate(df, self._columns)
        
        if not valid_columns:
            return df
        
        df = df.withColumns({col: (F.col(col) / 1e3).cast(TimestampType()) for col in valid_columns})

        return df   