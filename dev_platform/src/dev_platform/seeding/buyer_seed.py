"""Generate small, sporadic synthetic buyer batches and append them to the landing volume.

Runs ON Databricks (serverless/cluster): writes parquet straight to the UC volume
with Spark — no CLI, no extra dependencies. Like the salesperson seed, a run may write
nothing: each run draws against ``load_probability`` and only then appends
1..``max_batch_size`` rows, so the raw ingestion sees an irregular arrival pattern.

The Spark schema mirrors the buyer landing parquet (order + types). Buyers are Purchasing
department employees (Buyer / Purchasing Assistant / Purchasing Manager), one row per
employee here — the real landing data also repeats an employee per department and pay history.
"""

from __future__ import annotations

import datetime as dt
import random
import time
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SPARK_SCHEMA = StructType(
    [
        StructField("businessentityid", IntegerType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("loginid", StringType()),
        StructField("birthdate", DateType()),
        StructField("maritalstatus", StringType()),
        StructField("gender", StringType()),
        StructField("hiredate", DateType()),
        StructField("jobtitle", StringType()),
        StructField("departmentid", ShortType()),
        StructField("name", StringType()),
        StructField("groupname", StringType()),
        StructField("payfrequency", IntegerType()),
        StructField("rate", DecimalType(19, 4)),
        StructField("ratechangedate", TimestampType()),
        StructField("startdate", DateType()),
        StructField("enddate", DateType()),
        StructField("salariedflag", BooleanType()),
        StructField("currentflag", BooleanType()),
        StructField("inserted_date", TimestampType()),
        StructField("modified_date", StringType()),
        StructField("eltversion", StringType()),
    ]
)

_FIRST = ["Sheela", "Ann", "Reinout", "Hung-Fu", "Eric", "Linda", "Annette", "Rob", "Mikael", "Fukiko"]
_LAST = ["Word", "Hill", "Hagens", "Ting", "Kurjan", "Meisner", "Hill", "Walters", "Sandberg", "Ogisu"]
# jobtitle -> (weight, salaried). Buyers are the bulk; managers are salaried, the rest hourly.
_TITLES = {
    "Buyer": (0.70, False),
    "Purchasing Assistant": (0.15, False),
    "Purchasing Manager": (0.15, True),
}
_PURCHASING_DEPARTMENT_ID = 5
_PURCHASING_DEPARTMENT = "Purchasing"
_PURCHASING_GROUP = "Inventory Management"


def _row(entity_id: int, rng: random.Random) -> dict:
    first, last = rng.choice(_FIRST), rng.choice(_LAST)
    hired = dt.date(rng.randint(2005, 2014), rng.randint(1, 12), rng.randint(1, 28))
    jobtitle = rng.choices(list(_TITLES), weights=[w for w, _ in _TITLES.values()])[0]
    return {
        "businessentityid": entity_id,
        "firstname": first,
        "lastname": last,
        "loginid": f"adventure-works\\{first.lower()}{rng.randint(0, 99)}",
        "birthdate": dt.date(rng.randint(1960, 1995), rng.randint(1, 12), rng.randint(1, 28)),
        "maritalstatus": rng.choice(["M", "S"]),
        "gender": rng.choice(["M", "F"]),
        "hiredate": hired,
        "jobtitle": jobtitle,
        "departmentid": _PURCHASING_DEPARTMENT_ID,
        "name": _PURCHASING_DEPARTMENT,
        "groupname": _PURCHASING_GROUP,
        "payfrequency": rng.choice([1, 2]),
        "rate": Decimal(f"{rng.randint(9, 60)}.{rng.randint(0, 99):02d}"),
        "ratechangedate": dt.datetime.combine(hired, dt.time()),
        "startdate": hired,
        "enddate": None,
        "salariedflag": _TITLES[jobtitle][1],
        "currentflag": True,
        "inserted_date": dt.datetime.now(),
        "modified_date": "9999-31-12 00:00:00.000",
        "eltversion": "1",
    }


def generate_rows(n: int, base_id: int, rng: random.Random | None = None) -> list[dict]:
    """n synthetic buyer rows with contiguous ids starting at base_id."""
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
    """Maybe append one small parquet file of synthetic buyers to ``volume_path``.

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
        base_id = 4_000_000 + int(time.time()) % 50_000_000
    df = spark.createDataFrame(generate_rows(n, base_id, rng), schema=SPARK_SCHEMA)
    df.coalesce(1).write.mode("append").parquet(volume_path)
    return base_id, n
