"""Contrato de schema — afirma colunas/tipos da saída trusted do customer.

Aplica a cadeia de transformers da factory (sem reader/writer) sobre um df
raw-like e verifica o contrato da trusted: chave long, phonenumber string,
HomeOwnerFlag boolean e nenhuma coluna de metadados de origem sobrando.
"""

from unittest.mock import MagicMock

from pyspark.sql.types import LongType, StringType, StructField, StructType

from dev_platform.domains.customer.pipeline import RAW_METADATA_COLUMNS, build_customer_trusted


def test_trusted_schema_contract(spark):
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
        ]
        + [StructField(c, StringType()) for c in RAW_METADATA_COLUMNS]
    )
    row = (1, "a@b.com", "NY", "NY", "john", "doe", "230-555-0191", "true") + tuple(
        "x" for _ in RAW_METADATA_COLUMNS
    )
    df = spark.createDataFrame([row], schema)

    # reader/writer não são exercitados; só a lista de transformers da factory.
    pipeline = build_customer_trusted(MagicMock(), "advworks_dev")
    for transformer in pipeline.transformers:
        df = transformer.transform(df)

    dtypes = dict(df.dtypes)
    assert dtypes["customerid"] == "bigint"
    assert dtypes["phonenumber"] == "string"
    assert dtypes["HomeOwnerFlag"] == "boolean"
    for col in RAW_METADATA_COLUMNS:
        assert col not in df.columns, f"coluna de origem não deveria sobrar: {col}"
