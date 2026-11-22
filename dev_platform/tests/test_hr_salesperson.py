"""HR salesperson rules and trusted pipeline — small in-memory DataFrames, asserting the output.

Rule tests use the layout the rules receive in the pipeline: after CleanStringsTransformer,
so strings are lowercase. Managers come with every region/quota field null and bonus/commission
at 0; representatives carry the values the manager rules derive from.
"""

import datetime as dt
from decimal import Decimal as D
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from dev_platform.domains.humanresources.hr_salesperson_pipeline import build_hr_salesperson_trusted
from dev_platform.domains.humanresources.rules.salesmanagerbonus import CalculateSalesManagerBonus
from dev_platform.domains.humanresources.rules.salesmanagercommissioncalculate import CalculateSalesManagerCommission
from dev_platform.domains.humanresources.rules.salesmanagercountry import FillSalesManagerCountry
from dev_platform.domains.humanresources.rules.salesmanagerquota import CalculateSalesManagerQuota
from dev_platform.domains.humanresources.rules.salesmanagerregion import FillSalesManagerRegion
from dev_platform.domains.humanresources.rules.salesmanagerterritory import FillSalesManagerTerritory
from dev_platform.transformers.extractlogin import ExtractPrefixLogin

REP = "sales representative"
NA, EU, PAC = "north american sales manager", "european sales manager", "pacific sales manager"
MANAGERS = [NA, EU, PAC]

MONEY = DecimalType(19, 4)
RULES_SCHEMA = StructType(
    [
        StructField("jobtitle", StringType()),
        StructField("territoryid", IntegerType()),
        StructField("regionname", StringType()),
        StructField("continentname", StringType()),
        StructField("countryregioncode", StringType()),
        StructField("salesquota", MONEY),
        StructField("bonus", MONEY),
        StructField("commissionpct", DecimalType(10, 4)),
    ]
)


def _salesperson(spark: SparkSession) -> DataFrame:
    rows = [
        (REP, 7, "france", "europe", "fr", D(250000), D(2000), D("0.012")),
        (REP, 8, "germany", "europe", "de", D(250000), D(4000), D("0.010")),
        (REP, 9, "australia", "pacific", "au", D(250000), D(5000), D("0.018")),
        (REP, 1, "northwest", "north america", "us", D(300000), D(4100), D("0.015")),
        (REP, 2, "northeast", "north america", "us", D(300000), D(3550), D("0.010")),
        (REP, 6, "canada", "north america", "ca", D(250000), D(900), D("0.012")),
        (EU, None, None, None, None, None, D(0), D(0)),
        (PAC, None, None, None, None, None, D(0), D(0)),
        (NA, None, None, None, None, None, D(0), D(0)),
    ]
    return spark.createDataFrame(rows, RULES_SCHEMA)


def _managers(df: DataFrame, column: str) -> dict[str, Any]:
    return {r["jobtitle"]: r[column] for r in df.where(df.jobtitle.isin(MANAGERS)).collect()}


def _assert_reps_untouched(before: DataFrame, after: DataFrame) -> None:
    assertDataFrameEqual(after.where(after.jobtitle == REP), before.where(before.jobtitle == REP))


TERRITORY = {"north american": 1, "pacific sales": 9, "european sales": 8}
COUNTRY = {"north american": "us", "pacific sales": "au", "european sales": "de"}


def test_fill_territory(spark: SparkSession):
    df = _salesperson(spark)
    out = FillSalesManagerTerritory(TERRITORY).transform(df)
    assert _managers(out, "territoryid") == {NA: 1, EU: 8, PAC: 9}
    _assert_reps_untouched(df, out)


def test_fill_country_needs_territory_first(spark: SparkSession):
    df = _salesperson(spark)
    rule = FillSalesManagerCountry(COUNTRY)

    # managers still have no territory: the country rule does nothing yet
    assert _managers(rule.transform(df), "countryregioncode") == {NA: None, EU: None, PAC: None}

    out = rule.transform(FillSalesManagerTerritory(TERRITORY).transform(df))
    assert _managers(out, "countryregioncode") == {NA: "us", EU: "de", PAC: "au"}


def test_fill_region(spark: SparkSession):
    df = _salesperson(spark)
    out = FillSalesManagerRegion(
        {"north american": "usa hq", "european sales": "berlin hq", "pacific sales": "sydney hq"}
    )
    out = out.transform(df)
    assert _managers(out, "regionname") == {NA: "usa hq", EU: "berlin hq", PAC: "sydney hq"}
    _assert_reps_untouched(df, out)


def test_commission_fills_zero_only_for_managers(spark: SparkSession):
    df = _salesperson(spark)
    out = CalculateSalesManagerCommission({"north american": 0.05, "european sales": 0.04, "pacific sales": 0.04})
    out = out.transform(df)
    assert _managers(out, "commissionpct") == {NA: D("0.0500"), EU: D("0.0400"), PAC: D("0.0400")}
    _assert_reps_untouched(df, out)


def test_quota_is_region_max_plus_uplift(spark: SparkSession):
    df = _salesperson(spark)
    rules = {
        "north american": ("north america", 0.70),
        "european sales": ("europe", 0.65),
        "pacific sales": ("pacific", 0.55),
    }
    out = CalculateSalesManagerQuota(rules, region_column="continentname").transform(df)
    # max by region: north america 300k, europe 250k, pacific 250k
    assert _managers(out, "salesquota") == {NA: D("510000"), EU: D("412500"), PAC: D("387500")}
    _assert_reps_untouched(df, out)


def test_bonus_is_region_max_times_multiplier(spark: SparkSession):
    df = _salesperson(spark)
    rules = {
        "north american": ("north america", 2.70),
        "european sales": ("europe", 2.65),
        "pacific sales": ("pacific", 2.55),
    }
    out = CalculateSalesManagerBonus(rules).transform(df)
    # max bonus by region: north america 4100, europe 4000, pacific 5000
    assert _managers(out, "bonus") == {NA: D("11070"), EU: D("10600"), PAC: D("12750")}
    _assert_reps_untouched(df, out)


def test_rules_keep_values_that_are_already_set(spark: SparkSession):
    rows = [
        (REP, 1, "northwest", "north america", "us", D(300000), D(4100), D("0.015")),
        (NA, None, None, None, None, D(1), D(5), D("0.09")),
    ]
    df = spark.createDataFrame(rows, RULES_SCHEMA)

    out = CalculateSalesManagerQuota({"north american": ("north america", 0.70)}).transform(df)
    out = CalculateSalesManagerBonus({"north american": ("north america", 2.70)}).transform(out)
    out = CalculateSalesManagerCommission({"north american": 0.05}).transform(out)

    manager = out.where(out.jobtitle == NA).first()
    assert (manager.salesquota, manager.bonus, manager.commissionpct) == (D("1"), D("5"), D("0.09"))


def test_rules_without_regional_reference_leave_manager_unfilled(spark: SparkSession):
    df = spark.createDataFrame([(NA, None, None, None, None, None, D(0), D(0))], RULES_SCHEMA)

    out = CalculateSalesManagerQuota({"north american": ("north america", 0.70)}).transform(df)
    out = CalculateSalesManagerBonus({"north american": ("north america", 2.70)}).transform(out)

    manager = out.first()
    assert manager.salesquota is None
    assert manager.bonus == D("0")


def test_extract_login_keeps_the_user_after_the_domain(spark: SparkSession):
    df = spark.createDataFrame([("adventure-works\\pamela0",), ("no-domain",)], ["loginid"])
    out = ExtractPrefixLogin(["loginid"]).transform(df)
    assertDataFrameEqual(out, spark.createDataFrame([("pamela0",), ("no-domain",)], ["loginid"]))


# --- trusted pipeline ----------------------------------------------------------------------


def _unwrap(component: Any) -> Any:
    """Strips Logging* decorators to reach the real reader/transformer/writer."""
    while type(component).__name__.startswith("Logging"):
        component = next(
            getattr(component, attr) for attr in ("_reader", "_transformer", "_writer") if hasattr(component, attr)
        )
    return component


def test_trusted_wiring(spark: SparkSession):
    p = build_hr_salesperson_trusted(spark, "advworks_dev")
    assert p.name == "hr_salesperson_raw_to_trusted"
    assert type(_unwrap(p.reader)).__name__ == "TableReader"
    writer = _unwrap(p.writer)
    assert type(writer).__name__ == "TableMergeWriter"
    assert writer._keys == ["businessentityid"]

    names = [type(_unwrap(t)).__name__ for t in p.transformers]
    assert names.index("FillSalesManagerTerritory") < names.index("FillSalesManagerCountry")
    for rule in ("FillSalesManagerRegion", "CalculateSalesManagerCommission", "CalculateSalesManagerQuota"):
        assert rule in names
    assert "CalculateSalesManagerBonus" in names


RAW_SCHEMA = StructType(
    [
        StructField("businessentityid", IntegerType()),
        StructField("loginid", StringType()),
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("gender", StringType()),
        StructField("maritalstatus", StringType()),
        StructField("birthdate", DateType()),
        StructField("hiredate", DateType()),
        StructField("salariedflag", IntegerType()),
        StructField("currentflag", IntegerType()),
        StructField("groupname", StringType()),
        StructField("departmentname", StringType()),
        StructField("jobtitle", StringType()),
        StructField("territoryid", IntegerType()),
        StructField("regionname", StringType()),
        StructField("continentname", StringType()),
        StructField("countryregioncode", StringType()),
        StructField("salesquota", MONEY),
        StructField("bonus", MONEY),
        StructField("commissionpct", DecimalType(10, 4)),
    ]
)


def _raw_row(i: int, title: str, territory, region, continent, country, quota, bonus, commission) -> tuple:
    return (
        i, f"adventure-works\\user{i}", f" First{i} ", "Last", "M", "S",
        dt.date(1980, 5, 1), dt.date(2010, 3, 1), 1, 1, "Sales and Marketing", "Sales",
        title, territory, region, continent, country, quota, bonus, commission,
    )  # fmt: skip


class _StubReader:
    def __init__(self, df: DataFrame):
        self._df = df

    def read(self) -> DataFrame:
        return self._df


class _StubWriter:
    written: DataFrame | None = None

    def write(self, df: DataFrame) -> None:
        self.written = df


def test_trusted_pipeline_end_to_end(spark: SparkSession):
    rows = [
        _raw_row(1, "Sales Representative", 1, "Northwest", "North America", "US", D(300000), D(4100), D("0.015")),
        _raw_row(2, "Sales Representative", 8, "Germany", "Europe", "DE", D(250000), D(4000), D("0.010")),
        _raw_row(3, "Sales Representative", 9, "Australia", "Pacific", "AU", D(250000), D(5000), D("0.018")),
        _raw_row(4, "European Sales Manager", None, None, None, None, None, D(0), D(0)),
        _raw_row(5, "Pacific Sales Manager", None, None, None, None, None, D(0), D(0)),
        _raw_row(6, "North American Sales Manager", None, None, None, None, None, D(0), D(0)),
        _raw_row(None, "Sales Representative", 1, "Northwest", "North America", "US", D(300000), D(100), D("0.01")),
    ]
    writer = _StubWriter()
    pipeline = build_hr_salesperson_trusted(spark, "advworks_dev")
    pipeline.reader, pipeline.writer = _StubReader(spark.createDataFrame(rows, RAW_SCHEMA)), writer
    pipeline.run()

    out = {r["businessentityid"]: r for r in writer.written.collect()}
    assert set(out) == {1, 2, 3, 4, 5, 6}  # the row without businessentityid is filtered out
    assert out[1]["loginid"] == "user1"

    expected = {
        4: ("european sales manager", 8, "de", "berlin hq", D("0.04"), D("412500"), D("10600")),
        5: ("pacific sales manager", 9, "au", "sydney hq", D("0.04"), D("387500"), D("12750")),
        6: ("north american sales manager", 1, "us", "usa hq", D("0.05"), D("510000"), D("11070")),
    }
    for entity_id, values in expected.items():
        r = out[entity_id]
        got = (r.jobtitle, r.territoryid, r.countryregioncode, r.regionname, r.commissionpct, r.salesquota, r.bonus)
        assert got == values
