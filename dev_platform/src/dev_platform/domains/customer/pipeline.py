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
from dev_platform.core.common.reader import LoggingReader, TableReader, VolumeParquetReader
from dev_platform.core.common.transformer import LoggingTransformer
from dev_platform.core.common.writers import LoggingWriter, TableMergeWriter, TableWriter
from dev_platform.domains.customer.rules.customercreditranking import DeriveCustomerCreditRanking
from dev_platform.domains.customer.rules.customerincometransformer import DeriveCustomerIncome
from dev_platform.domains.customer.rules.customertypetransformer import DeriveCustomerType
from dev_platform.domains.customer.rules.phonetransformer import NormalizePhoneTransformer
from dev_platform.transformers.audit import AuditTransformer
from dev_platform.transformers.cleaning import CastTypesTransformer, CleanStringsTransformer
from dev_platform.transformers.dedup import DeduplicateTransformer
from dev_platform.transformers.email import EmailFlagTransformer
from dev_platform.transformers.filters import FilterInvalidTransformer
from dev_platform.transformers.metadata import DropColumnsTransformer, MetadataTransformer
from dev_platform.transformers.normalizeagebydate import DeriveAgeByDate
from dev_platform.transformers.normalizegender import NormalizeGender
from dev_platform.transformers.normalizemaritalstatus import NormalizeMaritalStatus

# Source system metadata columns that land in raw and do not belong in trusted.
RAW_METADATA_COLUMNS = [
    "inserted_date",
    "modified_date",
    "eltversion",
    "_rescued_data",
    "extracted_date",
    "__metadata__",
]


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
        reader=VolumeParquetReader.for_landing(
            spark, catalog=landing_catalog, dataset="customer", options={"mergeSchema": "true"}
        ),
        transformers=[
            MetadataTransformer(dataset="customer"),
            AuditTransformer(layer="raw"),
        ],
        writer=TableWriter(spark, table=f"{catalog}.raw.customer", mode="append", options={"mergeSchema": "true"}),
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
        reader=LoggingReader(TableReader(spark, table=f"{catalog}.raw.customer")),
        transformers=[
            # 1) type casting
            LoggingTransformer(CastTypesTransformer(schema_map={"HomeOwnerFlag": BooleanType()})),
            # 2) generic normalization
            LoggingTransformer(CleanStringsTransformer(columns=["city", "statename", "firstname", "lastname"])),
            # 3) customer domain-specific transformation
            LoggingTransformer(NormalizePhoneTransformer()),
            # 4) data quality + source metadata cleanup
            LoggingTransformer(
                FilterInvalidTransformer(
                    rules=[
                        col("customerid").isNotNull(),
                        col("emailaddress").contains("@"),
                    ]
                )
            ),
            # 5) one row per merge key BEFORE the merge (raw keeps one row per
            # customer-address); keep the latest ingestion. Runs before DropColumns
            # so inserted_date still exists as the tie-breaker.
            LoggingTransformer(DeduplicateTransformer(keys="customerid", order_by="inserted_date")),
            LoggingTransformer(DeriveAgeByDate(columns=["BirthDate"])),
            LoggingTransformer(DeriveCustomerIncome(columns=["YearlyIncome"])),
            LoggingTransformer(DeriveCustomerCreditRanking(columns=["minincome"])),
            LoggingTransformer(DeriveCustomerType(columns=["persontype"])),
            LoggingTransformer(NormalizeGender(columns=["Gender"])),
            LoggingTransformer(NormalizeMaritalStatus(columns=["MaritalStatus"])),
            LoggingTransformer(EmailFlagTransformer(columns=["emailaddress"])),
            LoggingTransformer(DropColumnsTransformer(columns=RAW_METADATA_COLUMNS)),
        ],
        writer=LoggingWriter(
            TableMergeWriter(
                spark,
                table=f"{catalog}.trusted.customer",
                merge_key="customerid",
                schema_evolution=True,
                # Liquid clustering on the merge key plus the region code: every MERGE
                # probes the table on customerid, so clustering by it speeds up file pruning.
                cluster_by=["customerid", "countryregioncode"],
            ),
        ),
    )
