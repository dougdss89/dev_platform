"""Unit de transformação — DataFrame pequeno in-memory, afirma a saída.

O tipo de teste mais usado em engenharia de dados: barato, rápido, sem cluster.
"""

from pyspark.testing import assertDataFrameEqual

from dev_platform.domains.customer.transformers import NormalizePhoneTransformer
from dev_platform.transformers.address import StandardizeAddressTransformer
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
