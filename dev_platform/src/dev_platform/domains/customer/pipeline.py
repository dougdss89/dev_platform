from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from dev_platform.core.io.readers import TableReader
from dev_platform.core.io.writers import TableMergeWriter
from dev_platform.core.pipeline import Pipeline
from dev_platform.domains.customer.transformers import NormalizePhoneTransformer
from dev_platform.transformers.cleaning import CastTypesTransformer, CleanStringsTransformer
from dev_platform.transformers.filters import FilterInvalidTransformer
from dev_platform.transformers.metadata import DropColumnsTransformer

# Colunas de ingestão presentes na raw (advworks.raw.customer) que não têm lugar na trusted.
RAW_METADATA_COLUMNS = ["inserted_date", "modified_date", "eltversion", "_rescued_data", "extracted_date"]


def build_customer_pipeline(spark: SparkSession, catalog: str) -> Pipeline:
    """Factory Method: encapsula como o pipeline raw->trusted de customer é montado.
    O caller só chama build_customer_pipeline(spark, catalog) e roda — não precisa
    saber quais transformers existem nem em que ordem.
    """
    return Pipeline(
        name="customer-raw-to-trusted",
        reader=TableReader(spark, table=f"{catalog}.raw.customer"),
        transformers=[
            CastTypesTransformer(schema_map={"HomeOwnerFlag": BooleanType()}),
            CleanStringsTransformer(columns=["city", "statename", "firstname", "lastname"]),
            NormalizePhoneTransformer(),
            FilterInvalidTransformer(
                rules=[
                    col("customerid").isNotNull(),
                    col("emailaddress").contains("@"),
                ]
            ),
            DropColumnsTransformer(columns=RAW_METADATA_COLUMNS),
        ],
        writer=TableMergeWriter(spark, table=f"{catalog}.trusted.customer", merge_key="customerid"),
    )
