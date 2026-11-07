"""Transformation unit tests — small in-memory DataFrame, asserting the output.

The most common test type in data engineering: cheap, fast, no cluster required.
"""

import datetime as dt

from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from dev_platform.domains.customer.rules.customercreditranking import DeriveCustomerCreditRanking
from dev_platform.domains.customer.rules.customerincometransformer import DeriveCustomerIncome
from dev_platform.domains.customer.rules.phonetransformer import NormalizePhoneTransformer
from dev_platform.transformers.address import StandardizeAddressTransformer
from dev_platform.transformers.dedup import DeduplicateTransformer
from dev_platform.transformers.normalizeagebydate import DeriveAgeByDate
from dev_platform.transformers.normalizegender import NormalizeGender
from dev_platform.transformers.normalizemaritalstatus import NormalizeMaritalStatus


def test_normalize_gender(spark: SparkSession):
    df = spark.createDataFrame([("M",), ("F",), ("X",), (None,)], ["gender"])
    out = NormalizeGender(columns=["gender"]).transform(df)
    expected = spark.createDataFrame([("male",), ("female",), ("Not Available",), ("Not Available",)], ["gender"])
    assertDataFrameEqual(out, expected)


def test_normalize_marital_status(spark: SparkSession):
    df = spark.createDataFrame([("M",), ("S",), ("D",), (None,)], ["maritalstatus"])
    out = NormalizeMaritalStatus(columns=["maritalstatus"]).transform(df)
    expected = spark.createDataFrame(
        [("married",), ("single",), ("Not Available",), ("Not Available",)], ["maritalstatus"]
    )
    assertDataFrameEqual(out, expected)


def test_standardize_address(spark: SparkSession):
    df = spark.createDataFrame([("123 Main St",)], ["addressline1"])
    row = StandardizeAddressTransformer().transform(df).first()
    assert row is not None
    assert row["addressline1"] == "123 Main Street"
    assert row["addressnumber"] == "123"
    assert row["custaddress"] == "Main Street"


def test_normalize_phone(spark: SparkSession):
    df = spark.createDataFrame([("230-555-0191",)], ["phonenumber"])
    out = NormalizePhoneTransformer().transform(df)
    row = out.first()
    assert row is not None
    assert row["phonenumber"] == "2305550191"


def test_dedup_keeps_latest_per_key(spark: SparkSession):
    # Two rows for customer 1 (keep the latest inserted_date), one for customer 2.
    df = spark.createDataFrame(
        [
            (1, "old@x.com", "2024-01-01"),
            (1, "new@x.com", "2024-06-01"),
            (2, "a@x.com", "2024-03-01"),
        ],
        ["customerid", "emailaddress", "inserted_date"],
    )
    out = DeduplicateTransformer(keys="customerid", order_by="inserted_date").transform(df)
    expected = spark.createDataFrame(
        [(1, "new@x.com", "2024-06-01"), (2, "a@x.com", "2024-03-01")],
        ["customerid", "emailaddress", "inserted_date"],
    )
    assertDataFrameEqual(out, expected)


INCOME_BANDS = [
    ("0-25000", 0, 25000, "low"),
    ("25001-50000", 25001, 50000, "middle-low"),
    ("50001-75000", 50001, 75000, "middle"),
    ("75001-100000", 75001, 100000, "upper-middle"),
    ("100001-150000", 100001, 150000, "high"),
    ("greater than 100000", 100001, None, "high"),
    (None, None, None, "Not Available"),
    ("abc", None, None, "Not Available"),
]


def test_derive_income_and_class_for_every_band(spark: SparkSession):
    # ANSI is on by default: malformed bands must yield null, not raise.
    df = spark.createDataFrame([(band,) for band, *_ in INCOME_BANDS], "YearlyIncome string")
    out = DeriveCustomerIncome(columns=["YearlyIncome"]).transform(df)
    out = DeriveCustomerCreditRanking(columns=["minincome"]).transform(out)

    assert dict(out.dtypes)["minincome"] == "decimal(18,2)"
    assert dict(out.dtypes)["maxincome"] == "decimal(18,2)"

    rows = {r["YearlyIncome"]: r for r in out.collect()}
    for band, expected_min, expected_max, expected_class in INCOME_BANDS:
        row = rows[band]
        assert (None if row["minincome"] is None else int(row["minincome"])) == expected_min, band
        assert (None if row["maxincome"] is None else int(row["maxincome"])) == expected_max, band
        assert row["incomeclass"] == expected_class, band


def test_credit_ranking_skips_non_decimal_column(spark: SparkSession):
    df = spark.createDataFrame([("25001",)], ["minincome"])
    out = DeriveCustomerCreditRanking(columns=["minincome"]).transform(df)
    assert "incomeclass" not in out.columns


def test_derive_age_by_date(spark: SparkSession):
    # Dates ~6 months either side of a 30th birthday keep the test stable on any day of the year.
    today = dt.date.today()
    base = dt.date(today.year - 30, today.month, min(today.day, 28))
    older = base - dt.timedelta(days=183)
    younger = base + dt.timedelta(days=183)
    df = spark.createDataFrame([(older,), (younger,), (None,)], "BirthDate date")

    out = DeriveAgeByDate(columns=["BirthDate"]).transform(df)

    assert dict(out.dtypes)["BirthDate_age"] == "int"
    assert [r["BirthDate_age"] for r in out.collect()] == [30, 29, None]


def test_derive_age_skips_non_date_column(spark: SparkSession):
    df = spark.createDataFrame([("1990-01-01",)], ["BirthDate"])
    out = DeriveAgeByDate(columns=["BirthDate"]).transform(df)
    assert "BirthDate_age" not in out.columns
