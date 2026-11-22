"""Generate small, sporadic synthetic salesperson batches and append them to the landing volume.

Runs ON Databricks (serverless/cluster): writes parquet straight to the UC volume
with Spark — no CLI, no extra dependencies. Unlike the customer seed, a run may write
nothing: each run draws against ``load_probability`` and only then appends
1..``max_batch_size`` rows, so the raw ingestion sees an irregular arrival pattern.

The Spark schema mirrors the salesperson landing parquet (order + types). Managers come
without territory, quota, bonus or commission, which is the gap the trusted manager
rules fill.
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

SPARK_SCHEMA = StructType(
    [
        StructField("businessentityid", IntegerType()),
        StructField("loginid", StringType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("gender", StringType()),
        StructField("birthdate", DateType()),
        StructField("hiredate", DateType()),
        StructField("jobtitle", StringType()),
        StructField("maritalstatus", StringType()),
        StructField("organizationlevel", IntegerType()),
        StructField("departmentid", IntegerType()),
        StructField("startdate", DateType()),
        StructField("enddate", DateType()),
        StructField("groupname", StringType()),
        StructField("departmentname", StringType()),
        StructField("payfrequency", IntegerType()),
        StructField("rate", DecimalType(19, 4)),
        StructField("ratechangedate", TimestampType()),
        StructField("commissionpct", DecimalType(10, 4)),
        StructField("salesquota", DecimalType(19, 4)),
        StructField("bonus", DecimalType(19, 4)),
        StructField("regionname", StringType()),
        StructField("continentname", StringType()),
        StructField("countryregioncode", StringType()),
        StructField("territoryid", IntegerType()),
        StructField("salariedflag", IntegerType()),
        StructField("currentflag", IntegerType()),
        StructField("inserted_date", TimestampType()),
        StructField("modified_date", StringType()),
        StructField("eltversion", StringType()),
    ]
)

_FIRST = ["Pamela", "Jae", "Rachel", "Tete", "Lynn", "Shu", "Garrett", "Tsvi", "Michael", "Ranjit"]
_LAST = ["Ansman-Wolfe", "Pak", "Valdez", "Mensa-Annan", "Tsoflias", "Ito", "Vargas", "Reiter", "Blythe", "Varkey"]
_MANAGER_TITLES = ["North American Sales Manager", "European Sales Manager", "Pacific Sales Manager"]
_MANAGER_SHARE = 0.25

# territoryid -> (region name, continent, country code), as in the AdventureWorks sales territories.
_TERRITORIES = {
    1: ("Northwest", "North America", "US"),
    2: ("Northeast", "North America", "US"),
    3: ("Central", "North America", "US"),
    4: ("Southwest", "North America", "US"),
    5: ("Southeast", "North America", "US"),
    6: ("Canada", "North America", "CA"),
    7: ("France", "Europe", "FR"),
    8: ("Germany", "Europe", "DE"),
    9: ("Australia", "Pacific", "AU"),
    10: ("United Kingdom", "Europe", "GB"),
}


def _row(entity_id: int, rng: random.Random) -> dict:
    first, last = rng.choice(_FIRST), rng.choice(_LAST)
    hired = dt.date(rng.randint(2005, 2014), rng.randint(1, 12), rng.randint(1, 28))
    territory_id = rng.choice(list(_TERRITORIES))
    region, continent, country = _TERRITORIES[territory_id]
    row = {
        "businessentityid": entity_id,
        "loginid": f"adventure-works\\{first.lower()}{rng.randint(0, 99)}",
        "firstname": first,
        "lastname": last,
        "gender": rng.choice(["M", "F"]),
        "birthdate": dt.date(rng.randint(1960, 1995), rng.randint(1, 12), rng.randint(1, 28)),
        "hiredate": hired,
        "jobtitle": "Sales Representative",
        "maritalstatus": rng.choice(["M", "S"]),
        "organizationlevel": 3,
        "departmentid": 3,
        "startdate": hired,
        "enddate": None,
        "groupname": "Sales and Marketing",
        "departmentname": "Sales",
        "payfrequency": rng.choice([1, 2]),
        "rate": Decimal(f"{rng.randint(10, 60)}.{rng.randint(0, 99):02d}"),
        "ratechangedate": dt.datetime.combine(hired, dt.time()),
        "commissionpct": Decimal(rng.randint(10, 20)) / Decimal(1000),
        "salesquota": Decimal(rng.choice([250000, 300000])),
        "bonus": Decimal(rng.randrange(0, 7000, 50)),
        "regionname": region,
        "continentname": continent,
        "countryregioncode": country,
        "territoryid": territory_id,
        "salariedflag": 0,
        "currentflag": 1,
        "inserted_date": dt.datetime.now(),
        "modified_date": "9999-31-12 00:00:00.000",
        "eltversion": "1",
    }
    if rng.random() < _MANAGER_SHARE:
        row.update(
            jobtitle=rng.choice(_MANAGER_TITLES),
            organizationlevel=2,
            territoryid=None,
            regionname=None,
            continentname=None,
            countryregioncode=None,
            salesquota=None,
            bonus=Decimal(0),
            commissionpct=Decimal(0),
        )
    return row


def generate_rows(n: int, base_id: int, rng: random.Random | None = None) -> list[dict]:
    """n synthetic salesperson rows with contiguous ids starting at base_id."""
    rng = rng or random.Random()
    return [_row(base_id + i, rng) for i in range(n)]


def seed_once(
    spark: SparkSession,
    volume_path: str,
    max_batch_size: int = 3,
    load_probability: float = 0.5,
    base_id: int | None = None,
    rng: random.Random | None = None,
) -> tuple[int, int] | None:
    """Maybe append one small parquet file of synthetic salespeople to ``volume_path``.

    Writes with probability ``load_probability``; otherwise does nothing and returns
    ``None``. A load has between 1 and ``max_batch_size`` rows. ``base_id`` defaults to a
    time-derived value so each load produces fresh ids (kept far above real ids, so they
    land as clean inserts). Returns (base_id, n) when it wrote.
    """
    rng = rng or random.Random()
    if rng.random() >= load_probability:
        return None
    n = rng.randint(1, max_batch_size)
    if base_id is None:
        base_id = 3_000_000 + int(time.time()) % 50_000_000
    df = spark.createDataFrame(generate_rows(n, base_id, rng), schema=SPARK_SCHEMA)
    df.coalesce(1).write.mode("append").parquet(volume_path)
    return base_id, n
