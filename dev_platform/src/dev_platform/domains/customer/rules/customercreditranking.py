import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DecimalType

from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer


class DeriveCustomerCreditRanking(BaseTransformer):
    """Classify each customer into an income class from the band's lower bound (e.g. minincome)."""

    def __init__(self, columns: list[str], validator: ColumnValidator = ColumnValidator((DecimalType,))):

        self._columns = columns
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:

        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        min_income = F.col(valid_columns[0])

        return df.withColumn(
            "incomeclass",
            F.when(min_income.isNull(), F.lit("Not Available"))
            .when(min_income < 25001, F.lit("low"))
            .when(min_income < 50001, F.lit("middle-low"))
            .when(min_income < 75001, F.lit("middle"))
            .when(min_income < 100001, F.lit("upper-middle"))
            .otherwise(F.lit("high")),
        )
