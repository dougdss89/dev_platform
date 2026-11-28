"""Data catalog — statement builders, the plan per table, and the real YAML files."""

from unittest.mock import MagicMock

import pytest

from dev_platform.governance.catalog import (
    apply_table_metadata,
    build_column_comment_sql,
    build_column_tags_sql,
    build_table_comment_sql,
    load_catalog,
    plan_metadata,
    validate_catalog,
)

TABLE = "c.trusted.buyer"
COMMON = {"_ingested_at": {"description": "Ingestion time."}}
ENTRY = {
    "description": "Buyers.",
    "columns": {
        "firstname": {"description": "First name.", "tags": {"pii": True}},
        "gender": {"description": "Gender."},
        "ghost": {"description": "Not in the table."},
    },
}


def test_builders_escape_quotes_and_backslashes():
    assert build_table_comment_sql(TABLE, "It's here") == "COMMENT ON TABLE c.trusted.buyer IS 'It''s here'"
    assert build_column_comment_sql(TABLE, "loginid", "adventure-works\\user") == (
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `loginid` COMMENT 'adventure-works\\\\user'"
    )


def test_column_tags_sql_stringifies_bool():
    sql = build_column_tags_sql(TABLE, "firstname", {"pii": True})
    assert sql == "ALTER TABLE c.trusted.buyer ALTER COLUMN `firstname` SET TAGS ('pii' = 'true')"


def test_plan_skips_columns_the_table_lacks_and_ignores_case():
    plan = plan_metadata(TABLE, ENTRY, ["FirstName", "gender", "_ingested_at"], COMMON)
    assert plan == [
        "COMMENT ON TABLE c.trusted.buyer IS 'Buyers.'",
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `_ingested_at` COMMENT 'Ingestion time.'",
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `FirstName` COMMENT 'First name.'",
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `FirstName` SET TAGS ('pii' = 'true')",
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `gender` COMMENT 'Gender.'",
    ]


def test_dataset_column_wins_over_common():
    entry = {"columns": {"_ingested_at": {"description": "Own."}}}
    assert plan_metadata(TABLE, entry, ["_ingested_at"], COMMON) == [
        "ALTER TABLE c.trusted.buyer ALTER COLUMN `_ingested_at` COMMENT 'Own.'"
    ]


def test_validate_reports_every_missing_description():
    bad = {"common": {"x": {}}, "datasets": {"buyer": {"columns": {"a": {"description": ""}, "b": None}}}}
    with pytest.raises(ValueError) as error:
        validate_catalog(bad)
    message = str(error.value)
    assert "_common.x" in message and "buyer: missing table description" in message
    assert "buyer.a" in message and "buyer.b" in message


def _spark(existing: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock()
    spark.catalog.tableExists.side_effect = lambda fqn: fqn in existing
    spark.table.side_effect = lambda fqn: MagicMock(columns=existing[fqn])
    return spark


def test_apply_runs_only_for_existing_tables():
    catalog = {"common": COMMON, "datasets": {"buyer": ENTRY, "supplier": ENTRY}}
    spark = _spark({"c.trusted.buyer": ["firstname", "_ingested_at"]})

    applied = apply_table_metadata(spark, "c", catalog, schema="trusted")

    assert applied == {"c.trusted.buyer": 4}
    assert spark.sql.call_count == 4


def test_dry_run_does_not_execute():
    catalog = {"common": {}, "datasets": {"buyer": ENTRY}}
    spark = _spark({"c.raw.buyer": ["firstname"]})
    assert apply_table_metadata(spark, "c", catalog, dry_run=True) == {"c.raw.buyer": 3}
    spark.sql.assert_not_called()


# --- the real files under conf/data_catalog ---------------------------------------------------


def test_repo_catalog_is_valid_and_has_the_pilot_dataset():
    catalog = load_catalog()
    assert "buyer" in catalog["datasets"]
    assert catalog["common"]  # audit and metadata columns


def test_every_buyer_landing_column_is_described():
    """Fails when the landing schema gains a column nobody described."""
    seed = pytest.importorskip("dev_platform.seeding.buyer_seed")
    catalog = load_catalog()
    described = {c.lower() for c in {**catalog["common"], **catalog["datasets"]["buyer"]["columns"]}}
    assert set(seed.SPARK_SCHEMA.fieldNames()) <= described
