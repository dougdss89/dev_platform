import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DateType

from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer


class DeriveAgeByDate(BaseTransformer):
    """Calculate the elapsed full years since each date column (e.g. age from a birth date)."""

    def __init__(
        self,
        columns: list[str],
        suffix: str = "_age",
        validator: ColumnValidator = ColumnValidator((DateType,)),
    ):

        self._columns = columns
        self._suffix = suffix
        self._validator = validator

    def transform(self, df: DataFrame) -> DataFrame:

        valid_columns = self._validator.validate(df, self._columns)

        if not valid_columns:
            return df

        return df.withColumns(
            {
                f"{name}{self._suffix}": F.floor(F.months_between(F.current_date(), F.col(name)) / 12).cast("int")
                for name in valid_columns
            }
        )
