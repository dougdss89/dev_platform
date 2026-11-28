""" Buyer pipeline.
    landing (parquet in volume)  --build_buyer_raw_ingestion-->  raw
    raw                          --build_buyer_trusted------->    trusted
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F
from dev_platform.core.common.pipeline import Pipeline
from dev_platform.core.common.reader import LoggingReader, TableReader, VolumeParquetReader
from dev_platform.core.common.transformer import LoggingTransformer
from dev_platform.core.common.writers import LoggingWriter, TableMergeWriter, TableWriter
from dev_platform.transformers.audit import AuditTransformer
from dev_platform.transformers.cleaning import CastTypesTransformer, CleanStringsTransformer
from dev_platform.transformers.dedup import DeduplicateTransformer
from dev_platform.transformers.extractlogin import ExtractPrefixLogin
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

def build_buyer_raw_ingestion(spark: SparkSession, catalog: str, landing_catalog: str = "example") -> Pipeline:
    
    """ 
    Pipeline landing to raw: persists data as-is + ingestion metadata
    """
    
    return Pipeline(
        name="buyer_landing_to_raw",
        
        reader = LoggingReader(VolumeParquetReader.for_landing(
            spark,
            catalog = landing_catalog,
            dataset= "buyer",
            options = {"mergeSchema": "true"},
        )),
        
        transformers=[LoggingTransformer(MetadataTransformer(dataset = "buyer")), 
                      LoggingTransformer(AuditTransformer(layer = "raw")),
                    ],
        
        writer = LoggingWriter(TableWriter(spark, 
                                           table = f"{catalog}.raw.buyer",
                                           mode = "append",
                                           options={"mergeSchema": "true"},))
    )
    
def build_buyer_trusted(spark: SparkSession, catalog: str) -> Pipeline:
    
    """ raw to trusted generic normalization """
    
    return Pipeline(
        name = "buyer_raw_to_trusted",
        reader = LoggingReader(TableReader(spark, table = f"{catalog}.raw.buyer")),
        writer = LoggingWriter(TableMergeWriter(spark,
                                                table = f"{catalog}.trusted.buyer",
                                                merge_key = "businessentityid",
                                                schema_evolution = True,
                                                cluster_by = ["jobtitle", "departmentid", "groupname"],
                                            )),
        
        transformers = [LoggingTransformer(DeduplicateTransformer(keys="businessentityid",
                                                                order_by=["inserted_date", 
                                                                          "startdate",
                                                                          "ratechangedate"])),

                        
                        LoggingTransformer(CastTypesTransformer(schema_map={"salariedflag": BooleanType(),
                                                                            "currentflag": BooleanType()})),
                       
                        LoggingTransformer(CleanStringsTransformer(["firstname",
                                                                   "lastname",
                                                                   "loginid",
                                                                   "maritalstatus",
                                                                   "gender",
                                                                   "jobtitle",
                                                                   "name",
                                                                   "groupname",])),
                       
                        LoggingTransformer(FilterInvalidTransformer(rules=[F.col("businessentityid").isNotNull(), 
                                                                           F.col("firstname").isNotNull(),
                                                                           F.col("birthdate").isNotNull(),
                                                                           F.col("hiredate").isNotNull()
                                                                        ])),
                        LoggingTransformer(NormalizeGender(["gender"])),
                        LoggingTransformer(NormalizeMaritalStatus(["maritalstatus"])),
                        LoggingTransformer(DeriveAgeByDate(["birthdate","hiredate"])),
                        LoggingTransformer(DropColumnsTransformer(columns=RAW_METADATA_COLUMNS)),
                        LoggingTransformer(ExtractPrefixLogin(["loginid"])),
                        
                ])