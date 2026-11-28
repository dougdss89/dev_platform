"""Buyer pipelines — wiring and the raw -> trusted run on small in-memory DataFrames.

The landing data repeats an employee per department and pay history (businessentityid 250 has
9 rows), so the trusted layer must collapse to one row per key before the MERGE.
"""

import datetime as dt
from decimal import Decimal as D
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from dev_platform.domains.buyer.buyer_pipeline import build_buyer_raw_ingestion, build_buyer_trusted
from dev_platform.seeding.buyer_seed import SPARK_SCHEMA


def _unwrap(component: Any) -> Any:
    """Strips Logging* decorators to reach the real reader/transformer/writer."""
    while type(component).__name__.startswith("Logging"):
        component = next(
            getattr(component, attr) for attr in ("_reader", "_transformer", "_writer") if hasattr(component, attr)
        )
    return component


class _StubReader:
    def __init__(self, df: DataFrame):
        self._df = df

    def read(self) -> DataFrame:
        return self._df


class _StubWriter:
    written: DataFrame | None = None

    def write(self, df: DataFrame) -> None:
        self.written = df


def _landing_row(entity_id: int | None, **overrides) -> dict:
    row = {
        "businessentityid": entity_id,
        "firstname": " Sheela ",
        "lastname": "Word",
        "loginid": f"adventure-works\\user{entity_id}",
        "birthdate": dt.date(1978, 2, 10),
        "maritalstatus": "S",
        "gender": "F",
        "hiredate": dt.date(2011, 2, 25),
        "jobtitle": "Purchasing Manager",
        "departmentid": 5,
        "name": "Purchasing",
        "groupname": "Inventory Management",
        "payfrequency": 2,
        "rate": D("30.0000"),
        "ratechangedate": dt.datetime(2012, 7, 13, 21, 0),
        "startdate": dt.date(2012, 7, 15),
        "enddate": None,
        "salariedflag": True,
        "currentflag": True,
        "inserted_date": dt.datetime(2026, 2, 27, 18, 58, 16),
        "modified_date": "9999-31-12 00:00:00.000",
        "eltversion": "1",
    }
    return row | overrides


def _run_trusted(spark: SparkSession, rows: list[dict]) -> list:
    writer = _StubWriter()
    pipeline = build_buyer_trusted(spark, "advworks_dev")
    pipeline.reader = _StubReader(spark.createDataFrame(rows, SPARK_SCHEMA))
    pipeline.writer = writer
    pipeline.run()
    return writer.written.collect()


def test_raw_wiring(spark: SparkSession):
    p = build_buyer_raw_ingestion(spark, "advworks_dev")
    assert p.name == "buyer_landing_to_raw"
    names = [type(_unwrap(t)).__name__ for t in p.transformers]
    assert names == ["MetadataTransformer", "AuditTransformer"]
    assert type(_unwrap(p.writer)).__name__ == "TableWriter"


def test_trusted_wiring(spark: SparkSession):
    p = build_buyer_trusted(spark, "advworks_dev")
    assert p.name == "buyer_raw_to_trusted"
    assert type(_unwrap(p.reader)).__name__ == "TableReader"
    writer = _unwrap(p.writer)
    assert type(writer).__name__ == "TableMergeWriter"
    assert writer._keys == ["businessentityid"]

    names = [type(_unwrap(t)).__name__ for t in p.transformers]
    assert names[0] == "DeduplicateTransformer"  # one row per key before anything is merged


def test_trusted_keeps_one_row_per_employee_with_the_current_record(spark: SparkSession):
    """Employee 250: 3 departments x 3 pay rates. The current Purchasing row with the latest rate wins."""
    history = [
        _landing_row(250, departmentid=4, name="Marketing", groupname="Sales and Marketing",
                     startdate=dt.date(2011, 2, 25), enddate=dt.date(2011, 7, 30), rate=D("22.5")),
        _landing_row(250, departmentid=13, name="Quality Assurance", groupname="Quality Assurance",
                     startdate=dt.date(2011, 7, 31), enddate=dt.date(2012, 7, 14), rate=D("30")),
        _landing_row(250, rate=D("9.8600"), ratechangedate=dt.datetime(2011, 2, 20, 21, 0)),
        _landing_row(250, rate=D("22.5000"), ratechangedate=dt.datetime(2011, 7, 29, 21, 0)),
        _landing_row(250, rate=D("30.0000"), ratechangedate=dt.datetime(2012, 7, 13, 21, 0)),
        _landing_row(251, jobtitle="Buyer", salariedflag=False),
    ]  # fmt: skip
    out = {r["businessentityid"]: r for r in _run_trusted(spark, history)}

    assert set(out) == {250, 251}
    current = out[250]
    assert (current.departmentid, current.name, current.rate) == (5, "purchasing", D("30.0000"))
    assert current.enddate is None


def test_trusted_is_stable_when_raw_is_reread(spark: SparkSession):
    """Raw is append-only and re-reads the whole landing, so the same row arrives again each run."""
    rows = [_landing_row(251, jobtitle="Buyer"), _landing_row(252, jobtitle="Buyer")]
    out = _run_trusted(spark, rows + rows)
    assert sorted(r["businessentityid"] for r in out) == [251, 252]


def test_trusted_normalizes_and_drops_source_metadata(spark: SparkSession):
    rows = [_landing_row(251, jobtitle=" Buyer ", gender="M", maritalstatus="M", salariedflag=False)]
    (r,) = _run_trusted(spark, rows)

    assert r.loginid == "user251"
    assert (r.firstname, r.jobtitle) == ("sheela", "buyer")
    assert (r.gender, r.maritalstatus) == ("male", "married")
    assert r.salariedflag is False and r.currentflag is True
    assert r.birthdate_age >= 47 and r.hiredate_age >= 14

    columns = set(r.asDict())
    assert not columns & {"inserted_date", "modified_date", "eltversion"}


def test_trusted_filters_invalid_rows(spark: SparkSession):
    rows = [
        _landing_row(251, jobtitle="Buyer"),
        _landing_row(None, jobtitle="Buyer"),
        _landing_row(252, jobtitle="Buyer", firstname=None),
        _landing_row(253, jobtitle="Buyer", birthdate=None),
        _landing_row(254, jobtitle="Buyer", hiredate=None),
    ]
    out = _run_trusted(spark, rows)
    assert [r["businessentityid"] for r in out] == [251]
