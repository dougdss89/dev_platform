"""Schema contract — asserts columns and types of the customer trusted output.

Applies the factory's transformer chain (without reader/writer) to a raw-like
DataFrame and verifies the trusted contract: long key, string phonenumber,
boolean HomeOwnerFlag, and no leftover source metadata columns.
"""

from unittest.mock import MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from dev_platform.domains.customer.pipeline import RAW_METADATA_COLUMNS, build_customer_trusted


def test_trusted_schema_contract(spark: SparkSession):
    schema = StructType(
        [
            StructField("customerid", LongType()),
            StructField("emailaddress", StringType()),
            StructField("city", StringType()),
            StructField("statename", StringType()),
            StructField("firstname", StringType()),
            StructField("lastname", StringType()),
            StructField("phonenumber", StringType()),
            StructField("HomeOwnerFlag", StringType()),
            StructField("YearlyIncome", StringType()),
        ]
        + [StructField(c, StringType()) for c in RAW_METADATA_COLUMNS]
    )
    row = (1, "a@b.com", "NY", "NY", "john", "doe", "230-555-0191", "true", "50001-75000") + tuple("x" for _ in RAW_METADATA_COLUMNS)
    df = spark.createDataFrame([row], schema)

    # reader/writer are not exercised; only the factory's transformer list is tested.
    pipeline = build_customer_trusted(MagicMock(), "advworks_dev")
    for transformer in pipeline.transformers:
        df = transformer.transform(df)

    dtypes = dict(df.dtypes)
    assert dtypes["customerid"] == "bigint"
    assert dtypes["phonenumber"] == "string"
    assert dtypes["HomeOwnerFlag"] == "boolean"
    assert dtypes["minincome"] == "decimal(18,2)"
    assert dtypes["maxincome"] == "decimal(18,2)"
    assert df.first()["incomeclass"] == "middle"
    for col in RAW_METADATA_COLUMNS:
        assert col not in df.columns, f"source metadata column should not remain: {col}"
