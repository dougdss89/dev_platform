"""Human resources department pipeline

Each factory wires a SINGLE ``Pipeline`` and nothing else — triggering
``.run()`` is the entrypoint's job, and the inter-layer order is defined by the job DAG.

    landing (parquet in volume)  --build_hr_department_raw_ingestion-->  raw
    raw                          --build_hr_department_trusted------->    trusted

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F
from dev_platform.core.common.pipeline import Pipeline
from dev_platform.core.common.reader import LoggingReader, TableReader, VolumeParquetReader
from dev_platform.core.common.writers import LoggingWriter, TableMergeWriter, TableWriter
from dev_platform.domains.humanresources.rules.salesmanagerbonus import CalculateSalesManagerBonus
from dev_platform.domains.humanresources.rules.salesmanagercommissioncalculate import CalculateSalesManagerCommission
from dev_platform.domains.humanresources.rules.salesmanagerquota import CalculateSalesManagerQuota
from dev_platform.domains.humanresources.rules.salesmanagerregion import FillSalesManagerRegion
from dev_platform.transformers.audit import AuditTransformer
from dev_platform.transformers.cleaning import CastTypesTransformer, CleanStringsTransformer
from dev_platform.transformers.extractlogin import ExtractPrefixLogin
from dev_platform.transformers.filters import FilterInvalidTransformer
from dev_platform.transformers.metadata import MetadataTransformer
from dev_platform.core.common.transformer import LoggingTransformer
from dev_platform.transformers.normalizeagebydate import DeriveAgeByDate
from dev_platform.transformers.normalizegender import NormalizeGender
from dev_platform.transformers.normalizemaritalstatus import NormalizeMaritalStatus

from dev_platform.domains.humanresources.rules.salesmanagerterritory import FillSalesManagerTerritory
from dev_platform.domains.humanresources.rules.salesmanagercountry import FillSalesManagerCountry

# Source system metadata columns that land in raw and do not belong in trusted.
RAW_METADATA_COLUMNS = [
    "inserted_date",
    "modified_date",
    "eltversion",
    "_rescued_data",
    "extracted_date",
    "__metadata__",
]


def build_hr_salesperson_raw_ingestion(spark: SparkSession, catalog: str, landing_catalog: str = "example") -> Pipeline:
    """Pipeline landing -> raw: persists data as-is + ingestion_metadata.
    No business rules here.
    Landing catalog is the catalog that handle volume data which is loaded
    by seeder job
    """

    return Pipeline(
        name="hr_salesperson_landing_to_raw",
        reader=VolumeParquetReader.for_landing(
            spark, catalog=landing_catalog, dataset="salesperson", options={"mergeSchema": "true"}
        ),
        transformers=[MetadataTransformer(dataset="salesperson"), AuditTransformer(layer="raw")],
        writer=TableWriter(spark, table=f"{catalog}.raw.salesperson", mode="append", options={"mergeSchema": "true"}),
    )


def build_hr_salesperson_trusted(spark: SparkSession, catalog: str) -> Pipeline:
    """raw -> trusted generic normalization + generic especic business rules"""

    return Pipeline(
        name="hr_salesperson_raw_to_trusted",
        reader=LoggingReader(TableReader(spark, table=f"{catalog}.raw.salesperson")),
        writer = LoggingWriter(TableMergeWriter(spark, 
                                                table=f"{catalog}.trusted.salesperson",
                                                merge_key="businessentityid",
                                                schema_evolution=True,
                                                cluster_by=["continentname", "countryregioncode"])),
        transformers=[
            LoggingTransformer(
                CastTypesTransformer(schema_map={"salariedflag": BooleanType(), 
                                                 "currentflag": BooleanType()})
            ),
            LoggingTransformer(
                CleanStringsTransformer(
                    [
                        "loginid",
                        "firstname",
                        "lastname",
                        "gender",
                        "jobtitle",
                        "maritalstatus",
                        "groupname",
                        "departmentname",
                        "regionname",
                        "continentname",
                        "countryregioncode",
                    ]
                )
            ),
            
            LoggingTransformer(FilterInvalidTransformer(rules=[F.col("businessentityid").isNotNull()])),
            LoggingTransformer(ExtractPrefixLogin(["loginid"])),
            LoggingTransformer(NormalizeGender(["gender"])),
            LoggingTransformer(DeriveAgeByDate(["birthdate", "hiredate"])),
            LoggingTransformer(NormalizeMaritalStatus(["maritalstatus"])),
            
            LoggingTransformer(FillSalesManagerTerritory({"north american": 1,
                                                            "pacific sales": 9,
                                                            "european sales": 8})),
            
            LoggingTransformer(FillSalesManagerCountry({"north american": "us",
                                                        "pacific sales": "au",
                                                        "european sales": "de"})),
            
            LoggingTransformer(FillSalesManagerRegion({ "north american": "usa hq",
                                                        "european sales": "berlin hq",
                                                        "pacific sales": "sydney hq"})),
            
            LoggingTransformer(CalculateSalesManagerCommission({"north american": 0.05,
                                                                "european sales": 0.04,
                                                                "pacific sales":  0.04})),

            LoggingTransformer(CalculateSalesManagerQuota({"north american": ("north america", 0.70),
                                                            "european sales": ("europe", 0.65),
                                                            "pacific sales": ("pacific", 0.55)},
                                                            region_column="continentname")),
            
            LoggingTransformer(CalculateSalesManagerBonus({"north american": ("north america", 2.70),
                                                        "european sales": ("europe", 2.65),
                                                        "pacific sales": ("pacific", 2.55)})),

        ])