"""Composition/wiring — asserts that factories wire the expected reader,
transformers, and writer. Uses the real session (`spark` fixture) because the
trusted factory evaluates `col(...)` expressions at construction time, which
requires an active SparkContext."""

from dev_platform.domains.customer.pipeline import (
    build_customer_raw_ingestion,
    build_customer_trusted,
)


def test_raw_ingestion_wiring(spark):
    p = build_customer_raw_ingestion(spark, "advworks_dev")
    assert p.name == "customer-landing-to-raw"
    assert type(p.reader).__name__ == "VolumeParquetReader"
    assert type(p.writer).__name__ == "TableWriter"
    assert [type(t).__name__ for t in p.transformers] == ["MetadataTransformer", "AuditTransformer"]


def test_trusted_wiring(spark):
    p = build_customer_trusted(spark, "advworks_dev")
    assert p.name == "customer-raw-to-trusted"
    assert type(p.reader).__name__ == "TableReader"
    assert type(p.writer).__name__ == "TableMergeWriter"
    assert p.writer._keys == ["customerid"]
    names = [type(t).__name__ for t in p.transformers]
    assert "NormalizePhoneTransformer" in names
    assert "DropColumnsTransformer" in names
