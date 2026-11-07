import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType

from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer


class DeriveCustomerIncome(BaseTransformer):
    """Transform values in YearlyIncome according to their ranges,
    deriving minincome and maxincome columns"""

    def __init__(self, columns: list[str], validator: ColumnValidator = ColumnValidator(StringType)):

        self._columns = columns
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:

        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        for column_name in valid_columns:
            parts = F.split(F.col(column_name), "-")

            df = df.withColumns(
                {
                    "minincome": F.coalesce(F.trim(parts.getItem(0)), F.lit("0")),
                    "maxincome": F.coalesce(F.trim(parts.getItem(1)), F.lit("0")),
                }
            )

        return df
