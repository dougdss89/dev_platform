"""Customer domain pipelines — one factory per medallion layer transition.

Molde para os demais domínios: a classe ``Pipeline`` é sempre a mesma; o que
muda em cada camada são as três estratégias injetadas (reader / transformers /
writer). Cada factory monta UM ``Pipeline`` e nada mais — quem dispara o
``.run()`` é o entrypoint, e a ordem entre camadas é o DAG do job.

    landing (parquet no volume)  --build_customer_raw_ingestion-->  raw
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
from dev_platform.transformers.filters import FilterInvalidTransformer
from dev_platform.transformers.metadata import DropColumnsTransformer, MetadataTransformer

# Colunas de metadados do sistema de origem que chegam na raw e não têm lugar na trusted.
RAW_METADATA_COLUMNS = ["inserted_date", "modified_date", "eltversion", "_rescued_data", "extracted_date", "__metadata__"]


def build_customer_raw_ingestion(
    spark: SparkSession,
    catalog: str,
    landing_catalog: str = "example",
) -> Pipeline:
    """landing -> raw: só persiste o dado "como chegou" + metadados de ingestão.

    Nenhuma regra de negócio aqui — a raw é o system-of-record auditável.
    ``landing_catalog`` é o catálogo onde está o volume de pouso (parquet),
    que pode ser diferente do catálogo de destino do projeto.
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
    """raw -> trusted: normalização genérica + tratamentos especializados do domínio.

    A ORDEM da lista é deliberada (cada transformer recebe o df do anterior):
      1) tipagem      -> antes de qualquer validação/normalização por tipo;
      2) normalização genérica (vem de ``transformers/``, reusável);
      3) especializados do domínio (vêm de ``domains/customer/``);
      4) filtros de qualidade e descarte de colunas de origem.
    """
    return Pipeline(
        name="customer-raw-to-trusted",
        reader=TableReader(spark, table=f"{catalog}.raw.customer"),
        transformers=[
            # 1) tipagem
            CastTypesTransformer(schema_map={"HomeOwnerFlag": BooleanType()}),
            # 2) normalização genérica
            CleanStringsTransformer(columns=["city", "statename", "firstname", "lastname"]),
            # 3) especializado do domínio customer
            NormalizePhoneTransformer(),
            # 4) qualidade + limpeza de metadados de origem
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
