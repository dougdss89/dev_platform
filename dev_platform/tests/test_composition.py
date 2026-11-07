"""Composition/wiring — asserts that factories wire the expected reader,
transformers, and writer. Uses the real session (`spark` fixture) because the
trusted factory evaluates `col(...)` expressions at construction time, which
requires an active SparkContext."""

from typing import Any

from pyspark.sql import SparkSession

from dev_platform.domains.customer.pipeline import (
    build_customer_raw_ingestion,
    build_customer_trusted,
)


def test_raw_ingestion_wiring(spark: SparkSession):
    p = build_customer_raw_ingestion(spark, "advworks_dev")
    assert p.name == "customer-landing-to-raw"
    assert type(p.reader).__name__ == "VolumeParquetReader"
    assert type(p.writer).__name__ == "TableWriter"
    assert [type(t).__name__ for t in p.transformers] == ["MetadataTransformer", "AuditTransformer"]


def _unwrap(component: Any) -> Any:
    """Strips Logging* decorators to reach the real reader/transformer/writer."""
    while type(component).__name__.startswith("Logging"):
        component = next(
            getattr(component, attr) for attr in ("_reader", "_transformer", "_writer") if hasattr(component, attr)
        )
    return component


def test_trusted_wiring(spark: SparkSession):
    p = build_customer_trusted(spark, "advworks_dev")
    assert p.name == "customer-raw-to-trusted"
    assert type(_unwrap(p.reader)).__name__ == "TableReader"
    writer = _unwrap(p.writer)
    assert type(writer).__name__ == "TableMergeWriter"
    assert writer._keys == ["customerid"]
    names = [type(_unwrap(t)).__name__ for t in p.transformers]
    assert "NormalizePhoneTransformer" in names
    assert "DropColumnsTransformer" in names
