"""Writer layout rules — pure, no Spark session needed."""

import sys
import types
from unittest.mock import MagicMock

import pytest

from dev_platform.core.common.writers import TableMergeWriter, _normalize_layout


def test_layout_defaults_empty():
    assert _normalize_layout(None, None) == ([], [])


def test_layout_partition_only():
    assert _normalize_layout(["dt"], None) == (["dt"], [])


def test_layout_cluster_only():
    assert _normalize_layout(None, ["customerid"]) == ([], ["customerid"])


def test_layout_partition_and_cluster_conflict():
    with pytest.raises(ValueError, match="mutually exclusive"):
        _normalize_layout(["dt"], ["customerid"])


def _fake_delta(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # Delta comes from the Databricks runtime and is not installed locally.
    delta = types.ModuleType("delta")
    tables = types.ModuleType("delta.tables")
    tables.DeltaTable = MagicMock()  # type: ignore[attr-defined]
    delta.tables = tables  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "delta", delta)
    monkeypatch.setitem(sys.modules, "delta.tables", tables)
    return tables.DeltaTable  # type: ignore[attr-defined,no-any-return]


@pytest.mark.parametrize("schema_evolution", [True, False])
def test_merge_schema_evolution_uses_builder_not_session_conf(monkeypatch: pytest.MonkeyPatch, schema_evolution: bool):
    delta_table = _fake_delta(monkeypatch)
    merge = delta_table.forName.return_value.alias.return_value.merge.return_value
    evolved = merge.withSchemaEvolution.return_value
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True

    TableMergeWriter(spark, table="c.s.t", merge_key="id", schema_evolution=schema_evolution).write(MagicMock())

    # serverless rejects spark.databricks.delta.schema.autoMerge.enabled
    spark.conf.set.assert_not_called()
    if schema_evolution:
        merge.withSchemaEvolution.assert_called_once()
        evolved.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once()
    else:
        merge.withSchemaEvolution.assert_not_called()
        merge.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once()
