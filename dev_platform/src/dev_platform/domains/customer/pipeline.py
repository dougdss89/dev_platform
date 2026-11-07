"""Customer domain pipelines — one factory per medallion layer transition.

Template for other domains: the ``Pipeline`` class is always the same; what
changes in each layer are the three injected strategies (reader / transformers /
writer). Each factory wires a SINGLE ``Pipeline`` and nothing else — triggering
``.run()`` is the entrypoint's job, and the inter-layer order is defined by the job DAG.

    landing (parquet in volume)  --build_customer_raw_ingestion-->  raw
    raw                          --build_customer_trusted------->    trusted
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

from dev_platform.core.common.pipeline import Pipeline
from dev_platform.core.common.reader import TableReader, VolumeParquetReader
from dev_platform.core.common.writers import TableMergeWriter, TableWriter
from dev_platform.domains.customer.transformers import NormalizePhoneTransformer
from dev_platform.transformers.audit import AuditTransformer
from dev_platform.transformers.cleaning import CastTypesTransformer, CleanStringsTransformer
from dev_platform.transformers.dedup import DeduplicateTransformer
from dev_platform.transformers.filters import FilterInvalidTransformer
from dev_platform.transformers.metadata import DropColumnsTransformer, MetadataTransformer

# Source system metadata columns that land in raw and do not belong in trusted.
RAW_METADATA_COLUMNS = ["inserted_date", "modified_date", "eltversion", "_rescued_data", "extracted_date", "__metadata__"]


def build_customer_raw_ingestion(
    spark: SparkSession,
    catalog: str,
    landing_catalog: str = "example",
) -> Pipeline:
    """landing -> raw: persists data as-is + ingestion metadata.

    No business rules here — raw is the auditable system of record.
    ``landing_catalog`` is the catalog hosting the landing volume (parquet),
    which may differ from the project's target catalog.
    """
    return Pipeline(
        name="customer-landing-to-raw",
        reader=VolumeParquetReader.for_landing(spark, catalog=landing_catalog, dataset="customer"),
        transformers=[
            MetadataTransformer(dataset="customer"),
            AuditTransformer(layer="raw"),
        ],
        writer=TableWriter(spark, table=f"{catalog}.raw.customer", mode="append"),
    )


def build_customer_trusted(spark: SparkSession, catalog: str) -> Pipeline:
    """raw -> trusted: generic normalization + domain-specific transformations.

    The list ORDER is deliberate (each transformer receives the DataFrame from the previous one):
      1) type casting -> before any type-dependent validation or normalization;
      2) generic normalization (from ``transformers/``, reusable);
      3) domain-specific transformations (from ``domains/customer/``);
      4) quality filters and dropping source metadata columns.
    """
    return Pipeline(
        name="customer-raw-to-trusted",
        reader=TableReader(spark, table=f"{catalog}.raw.customer"),
        transformers=[
            # 1) type casting
            CastTypesTransformer(schema_map={"HomeOwnerFlag": BooleanType()}),
            # 2) generic normalization
            CleanStringsTransformer(columns=["city", "statename", "firstname", "lastname"]),
            # 3) customer domain-specific transformation
            NormalizePhoneTransformer(),
            # 4) data quality + source metadata cleanup
            FilterInvalidTransformer(
                rules=[
                    col("customerid").isNotNull(),
                    col("emailaddress").contains("@"),
                ]
            ),
            # 5) one row per merge key BEFORE the merge (raw keeps one row per
            # customer-address); keep the latest ingestion. Runs before DropColumns
            # so inserted_date still exists as the tie-breaker.
            DeduplicateTransformer(keys="customerid", order_by="inserted_date"),
            DropColumnsTransformer(columns=RAW_METADATA_COLUMNS),
        ],
        writer=TableMergeWriter(spark, table=f"{catalog}.trusted.customer", merge_key="customerid"),
    )
