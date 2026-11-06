"""Generate small synthetic customer batches and append them to the landing volume.

Runs ON Databricks (serverless/cluster): writes parquet straight to the UC volume
with Spark — no CLI, no extra dependencies. The Spark schema mirrors the existing
landing parquet so the raw ingestion consumes it unchanged. Kept tiny for cost.
"""

from __future__ import annotations

import datetime as dt
import random
import time
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Matches example.raw.landing_data/customer (order + types).
SPARK_SCHEMA = StructType(
    [
        StructField("businessentityid", IntegerType()),
        StructField("customerid", IntegerType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("persontype", StringType()),
        StructField("emailpromotion", IntegerType()),
        StructField("typephone", StringType()),
        StructField("phonenumber", StringType()),
        StructField("postalcode", StringType()),
        StructField("addresstype", StringType()),
        StructField("emailaddress", StringType()),
        StructField("territoryid", IntegerType()),
        StructField("CountryRegionName", StringType()),
        StructField("stateprovinceid", IntegerType()),
        StructField("countryregioncode", StringType()),
        StructField("stateprovincecode", StringType()),
        StructField("statename", StringType()),
        StructField("city", StringType()),
        StructField("addressline1", StringType()),
        StructField("addressline2", StringType()),
        StructField("TotalPurchaseYTD", DecimalType(12, 2)),
        StructField("DateFirstPurchase", DateType()),
        StructField("BirthDate", DateType()),
        StructField("MaritalStatus", StringType()),
        StructField("YearlyIncome", StringType()),
        StructField("Gender", StringType()),
        StructField("TotalChildren", IntegerType()),
        StructField("NumberChildrenAtHome", IntegerType()),
        StructField("Education", StringType()),
        StructField("Occupation", StringType()),
        StructField("HomeOwnerFlag", StringType()),
        StructField("NumberCarsOwned", IntegerType()),
        StructField("inserted_date", TimestampType()),
        StructField("modified_date", StringType()),
        StructField("eltversion", StringType()),
    ]
)

_FIRST = ["Calvin", "Maria", "John", "Ana", "Liu", "Omar", "Sophie", "Diego", "Ingrid", "Noah"]
_LAST = ["Raji", "Silva", "Smith", "Costa", "Wang", "Hassan", "Martin", "Lopez", "Berg", "Khan"]
_STATES = [("WA ", "Washington", "Bothell"), ("CA ", "California", "San Diego"), ("OR ", "Oregon", "Portland")]
_STREETS = ["San Gabriel Dr.", "Main St.", "Oak Rd.", "Elm Blvd.", "Lincoln Ave.", "Pine Ct."]
_INCOME = ["0-25000", "25001-50000", "50001-75000", "75001-100000", "100001-150000"]
_EDU = ["High School ", "Bachelors ", "Graduate Degree ", "Partial College "]
_OCC = ["Professional", "Management", "Skilled Manual", "Clerical", "Manual"]


def _row(cid: int) -> dict:
    first, last = random.choice(_FIRST), random.choice(_LAST)
    code, name, city = random.choice(_STATES)
    return {
        "businessentityid": cid,
        "customerid": cid,
        "firstname": first,
        "lastname": last,
        "persontype": "IN",
        "emailpromotion": random.choice([0, 1, 2]),
        "typephone": random.choice(["Cell", "Home", "Work"]),
        "phonenumber": f"{random.randint(200, 999)}-555-{random.randint(1000, 9999)}",
        "postalcode": f"{random.randint(10000, 99999)}",
        "addresstype": random.choice(["Shipping", "Main Office", "Home"]),
        "emailaddress": f"{first.lower()}{random.randint(1, 99)}@adventure-works.com",
        "territoryid": random.randint(1, 10),
        "CountryRegionName": "United States",
        "stateprovinceid": random.randint(1, 80),
        "countryregioncode": "US",
        "stateprovincecode": code,
        "statename": name,
        "city": city,
        "addressline1": f"{random.randint(100, 9999)} {random.choice(_STREETS)}",
        "addressline2": None,
        "TotalPurchaseYTD": Decimal(f"{random.randint(-500, 5000)}.{random.randint(0, 99):02d}"),
        "DateFirstPurchase": dt.date(random.randint(2001, 2008), random.randint(1, 12), random.randint(1, 28)),
        "BirthDate": dt.date(random.randint(1950, 1995), random.randint(1, 12), random.randint(1, 28)),
        "MaritalStatus": random.choice(["M", "S"]),
        "YearlyIncome": random.choice(_INCOME),
        "Gender": random.choice(["M", "F"]),
        "TotalChildren": random.randint(0, 5),
        "NumberChildrenAtHome": random.randint(0, 3),
        "Education": random.choice(_EDU),
        "Occupation": random.choice(_OCC),
        "HomeOwnerFlag": random.choice(["0", "1"]),
        "NumberCarsOwned": random.randint(0, 4),
        "inserted_date": dt.datetime.now(),
        "modified_date": "9999-31-12 00:00:00.000",
        "eltversion": "1",
    }


def generate_rows(n: int, base_id: int) -> list[dict]:
    """n synthetic customer rows with contiguous ids starting at base_id."""
    return [_row(base_id + i) for i in range(n)]


def seed_once(
    spark: SparkSession,
    volume_path: str,
    batch_size: int = 8,
    base_id: int | None = None,
) -> tuple[int, int]:
    """Append one small parquet file of synthetic customers to ``volume_path``.

    ``base_id`` defaults to a time-derived value so each run produces fresh ids
    (kept far above real ids, so they land as clean inserts). Returns (base_id, n).
    """
    if base_id is None:
        base_id = 2_000_000 + int(time.time()) % 50_000_000
    rows = generate_rows(batch_size, base_id)
    df = spark.createDataFrame(rows, schema=SPARK_SCHEMA)
    df.coalesce(1).write.mode("append").parquet(volume_path)
    return base_id, batch_size
