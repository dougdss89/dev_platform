from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

class ColumnValidator:
    
    """ Validates dataframe column based on a specified data type """
    
    def __init__(self, expected_type: type[DataType] | tuple[type[DataType], ...]):
        self._expected_type = expected_type
        
    def validate(self, df: DataFrame, columns: list[str]) -> list[str]:
        
        return [
            col_name 
            for col_name in columns
            if col_name in df.schema.names
            and isinstance(df.schema[col_name].dataType, self._expected_type)
        ]
        
        