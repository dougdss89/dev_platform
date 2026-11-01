"""Unit tests for the governance tag helpers — pure functions, no Spark."""

from dev_platform.governance.tagging import build_set_tags_sql, resolve_tags

TAXONOMY = {
    "defaults": {"layer": "raw", "source_system": "adventureworks"},
    "datasets": {
        "customer": {
            "domain": "sales",
            "entity_type": "dimension",
            "sensitivity": "restricted",
            "contains_pii": True,
        },
    },
}


def test_resolve_merges_defaults_and_overrides_layer():
    tags = resolve_tags("customer", TAXONOMY, layer="trusted")
    assert tags["source_system"] == "adventureworks"
    assert tags["domain"] == "sales"
    assert tags["layer"] == "trusted"  # override wins over the default "raw"


def test_build_sql_stringifies_bool_and_quotes():
    sql = build_set_tags_sql("c.s.customer", {"contains_pii": True, "domain": "sales"})
    assert sql == "ALTER TABLE c.s.customer SET TAGS ('contains_pii' = 'true', 'domain' = 'sales')"
