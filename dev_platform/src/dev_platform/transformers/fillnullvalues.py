
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer

class FillNullValues(BaseTransformer):
    
    def __init__(self, 
                 columns:list[str],
                 validator: ColumnValidator = ColumnValidator(StringType)
            ):
        
        self._columns = columns
        self._validator = validator
        
    def transform(self, df: DataFrame) -> DataFrame:
        
        "Replace null values in string columns"
        
        valid_columns = self._validator.validate(df, self._columns)
        
        if not valid_columns:
            return df
        
        df = df.fillna({col_name: "Not Available" for col_name in valid_columns})
        
        return df