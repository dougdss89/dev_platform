
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer

class NormalizeString(BaseTransformer):
    
    """ Normalize all string values in the specified columns to lowercase.
    """
    
    def __init__(self, 
                 columns: list[str], 
                 validator: ColumnValidator = ColumnValidator(StringType)
                ):
        
        self._columns = columns
        self._validator = validator
        
    
    def transform(self, df: DataFrame) -> DataFrame:
        
        valid_columns = self._validator.validate(df, self._columns)
        
        if not valid_columns:
            return df
        
        df = df.withColumns({
                    col_name: F.lower(F.col(col_name)) for col_name in valid_columns
        })
        
        return df