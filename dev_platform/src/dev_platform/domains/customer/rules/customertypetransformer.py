import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType

from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer


class DeriveCustomerType(BaseTransformer):
    """Trasnform values in customertype according to their abbreviations"""

    def __init__(self, columns: list[str], validator: ColumnValidator = ColumnValidator(StringType)):

        self._columns = columns
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:

        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        for column_name in valid_columns:
            df = df.withColumn(
                column_name, F.when(F.col(column_name.lower()) == "in", "individual").otherwise(F.lit("Not Available"))
            )

        return df
