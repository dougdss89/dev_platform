import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from dev_platform.core.common.column_validator import ColumnValidator
from dev_platform.core.common.transformer import BaseTransformer

# One "@", something before it, and a dot with something after it in the domain.
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


class EmailFlagTransformer(BaseTransformer):
    """Adds a boolean <column>_is_valid flag for each e-mail column. Never changes or drops the
    original value. Null e-mails are flagged False. Leading/trailing whitespace is trimmed only
    for the check (the stored value is untouched); the flag is True only if the trimmed value
    matches EMAIL_PATTERN."""

    def __init__(
        self,
        columns: list[str],
        validator: ColumnValidator = ColumnValidator(StringType),
        suffix: str = "_is_valid",
    ):
        self._columns = columns
        self._validator = validator
        self._suffix = suffix

    def transform(self, df: DataFrame) -> DataFrame:
        for col_name in self._validator.validate(df, self._columns):
            is_valid = F.coalesce(F.trim(F.col(col_name)).rlike(EMAIL_PATTERN), F.lit(False))

            df = df.withColumn(f"{col_name}{self._suffix}", is_valid)

        return df
