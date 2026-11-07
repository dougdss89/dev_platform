"""Transformation unit tests — small in-memory DataFrame, asserting the output.

The most common test type in data engineering: cheap, fast, no cluster required.
"""

from pyspark.testing import assertDataFrameEqual

from dev_platform.domains.customer.transformers import NormalizePhoneTransformer
from dev_platform.transformers.address import StandardizeAddressTransformer
from dev_platform.transformers.dedup import DeduplicateTransformer
from dev_platform.transformers.normalizegender import NormalizeGender
from dev_platform.transformers.normalizemaritalstatus import NormalizeMaritalStatus


def test_normalize_gender(spark):
    df = spark.createDataFrame([("M",), ("F",), ("X",), (None,)], ["gender"])
    out = NormalizeGender(columns=["gender"]).transform(df)
    expected = spark.createDataFrame(
        [("male",), ("female",), ("Not Available",), ("Not Available",)], ["gender"]
    )
    assertDataFrameEqual(out, expected)


def test_normalize_marital_status(spark):
    df = spark.createDataFrame([("M",), ("S",), ("D",), (None,)], ["maritalstatus"])
    out = NormalizeMaritalStatus(columns=["maritalstatus"]).transform(df)
    expected = spark.createDataFrame(
        [("married",), ("single",), ("Not Available",), ("Not Available",)], ["maritalstatus"]
    )
    assertDataFrameEqual(out, expected)


def test_standardize_address(spark):
    df = spark.createDataFrame([("123 Main St",)], ["addressline1"])
    row = StandardizeAddressTransformer().transform(df).first()
    assert row["addressline1"] == "123 Main Street"
    assert row["addressnumber"] == "123"
    assert row["custaddress"] == "Main Street"


def test_normalize_phone(spark):
    df = spark.createDataFrame([("230-555-0191",)], ["phonenumber"])
    out = NormalizePhoneTransformer().transform(df)
    assert out.first()["phonenumber"] == "2305550191"


def test_dedup_keeps_latest_per_key(spark):
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
