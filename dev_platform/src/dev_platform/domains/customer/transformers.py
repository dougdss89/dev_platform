from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace

from dev_platform.core.common.transformer import BaseTransformer


class NormalizePhoneTransformer(BaseTransformer):
    """Strips phone number formatting: '230-555-0191' -> '2305550191'.
    Specific to the customer domain — only this entity has phonenumber.
    """

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("phonenumber", regexp_replace(col("phonenumber"), r"[^\d]", ""))
