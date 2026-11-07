import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, DecimalType

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
            numbers = F.regexp_extract_all(F.col(column_name), F.lit(r"(\d+)"), 1)
            first = F.try_element_at(numbers, F.lit(1)).try_cast(DecimalType(18, 2))
            second = F.try_element_at(numbers, F.lit(2)).try_cast(DecimalType(18, 2))
            is_open = F.col(column_name).startswith("greater than")

            df = df.withColumns(
                {
                    "minincome": F.when(is_open, first + F.lit(1)).otherwise(first).cast(DecimalType(18, 2)),
                    "maxincome": F.when(is_open, F.lit(None).cast(DecimalType(18, 2))).otherwise(second),
                }
            )

        return df
