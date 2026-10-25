from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace

from dev_platform.core.base import BaseTransformer


class NormalizePhoneTransformer(BaseTransformer):
    """Remove máscara de telefone: '230-555-0191' -> '2305550191'.
    Específico do domínio customer — só essa entidade tem phonenumber.
    """

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("phonenumber", regexp_replace(col("phonenumber"), r"[^\d]", ""))
