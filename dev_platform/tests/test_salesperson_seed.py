"""Salesperson seed — sporadic loads and the shape of the generated rows."""

import random
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from dev_platform.seeding.salesperson_seed import SPARK_SCHEMA, generate_rows, seed_once

MANAGER_TITLES = {"North American Sales Manager", "European Sales Manager", "Pacific Sales Manager"}


LANDING_SCHEMA_DDL = """
    businessentityid INT, loginid STRING, firstname STRING, lastname STRING, gender STRING,
    birthdate DATE, hiredate DATE, jobtitle STRING, maritalstatus STRING, organizationlevel INT,
    departmentid INT, startdate DATE, enddate DATE, groupname STRING, departmentname STRING,
    payfrequency INT, rate DECIMAL(19,4), ratechangedate TIMESTAMP, commissionpct DECIMAL(10,4),
    salesquota DECIMAL(19,4), bonus DECIMAL(19,4), regionname STRING, continentname STRING,
    countryregioncode STRING, territoryid INT, salariedflag INT, currentflag INT,
    inserted_date TIMESTAMP, modified_date STRING, eltversion STRING
"""


def test_schema_matches_landing_parquet():
    """Same order and types as the existing landing data, so new files merge with the old ones."""
    assert SPARK_SCHEMA == StructType.fromDDL(LANDING_SCHEMA_DDL)


def test_generate_rows_ids_and_columns(spark: SparkSession):
    rows = generate_rows(5, base_id=3_000_000, rng=random.Random(1))
    assert [r["businessentityid"] for r in rows] == list(range(3_000_000, 3_000_005))
    assert set(rows[0]) == set(SPARK_SCHEMA.fieldNames())
    assert spark.createDataFrame(rows, schema=SPARK_SCHEMA).count() == 5


def test_generate_rows_representatives_and_managers():
    rows = generate_rows(200, base_id=1, rng=random.Random(7))
    managers = [r for r in rows if r["jobtitle"] in MANAGER_TITLES]
    reps = [r for r in rows if r["jobtitle"] == "Sales Representative"]
    assert managers and reps and len(managers) + len(reps) == 200

    for m in managers:
        assert m["territoryid"] is None and m["salesquota"] is None
        assert m["bonus"] == 0 and m["commissionpct"] == 0
    for r in reps:
        assert r["territoryid"] is not None and r["salesquota"] is not None
        assert r["regionname"] and r["continentname"] and r["countryregioncode"]


def test_seed_once_without_load_writes_nothing(spark: SparkSession, tmp_path: Path):
    path = str(tmp_path / "salesperson")
    assert seed_once(spark, path, load_probability=0.0) is None
    assert not Path(path).exists()


def test_seed_once_with_load_appends_a_small_batch(spark: SparkSession, tmp_path: Path):
    path = str(tmp_path / "salesperson")
    result = seed_once(spark, path, max_batch_size=3, load_probability=1.0, base_id=3_000_000)
    assert result is not None
    base_id, n = result
    assert base_id == 3_000_000 and 1 <= n <= 3
    assert spark.read.parquet(path).count() == n


def test_seed_once_is_sporadic(spark: SparkSession, tmp_path: Path):
    path = str(tmp_path / "salesperson")
    rng = random.Random(42)
    outcomes = [seed_once(spark, path, load_probability=0.5, base_id=i * 10, rng=rng) for i in range(12)]
    loaded = [o for o in outcomes if o is not None]
    assert 0 < len(loaded) < len(outcomes)
    assert spark.read.parquet(path).count() == sum(n for _, n in loaded)
