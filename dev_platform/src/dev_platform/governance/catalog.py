"""Apply table/column descriptions and column tags as Unity Catalog metadata.

``conf/data_catalog/<dataset>.yml`` holds the description of a dataset and of each of its
columns (plus optional column tags such as ``pii``). ``conf/data_catalog/_common.yml`` holds
the columns every layer adds (audit and ingestion metadata). Both are the single source of
truth; this module turns them into ``COMMENT ON TABLE`` and ``ALTER COLUMN`` statements.

The statement builders and ``plan_metadata`` are pure (no Spark). ``apply_table_metadata``
runs them against the tables that exist. Re-running is safe: every statement overwrites.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from dev_platform.governance.tagging import _tag_value

COMMON_FILE = "_common.yml"


def _literal(text: str) -> str:
    """SQL string literal: Spark treats backslash as an escape, so escape it before the quotes."""
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def build_table_comment_sql(table_fqn: str, description: str) -> str:
    return f"COMMENT ON TABLE {table_fqn} IS {_literal(description)}"


def build_column_comment_sql(table_fqn: str, column: str, description: str) -> str:
    return f"ALTER TABLE {table_fqn} ALTER COLUMN {_ident(column)} COMMENT {_literal(description)}"


def build_column_tags_sql(table_fqn: str, column: str, tags: dict) -> str:
    pairs = ", ".join(f"'{k}' = '{_tag_value(v)}'" for k, v in tags.items())
    return f"ALTER TABLE {table_fqn} ALTER COLUMN {_ident(column)} SET TAGS ({pairs})"


def resolve_columns(entry: dict, common: dict | None = None) -> dict[str, dict]:
    """Common columns first, then the dataset's own (the dataset wins on a clash)."""
    return {**(common or {}), **entry.get("columns", {})}


def plan_metadata(table_fqn: str, entry: dict, table_columns: list[str], common: dict | None = None) -> list[str]:
    """Statements for one table. Columns the table does not have are skipped.

    Matching ignores case, and the statement uses the column's real name in the table.
    """
    statements = []
    if entry.get("description"):
        statements.append(build_table_comment_sql(table_fqn, entry["description"]))

    actual = {c.lower(): c for c in table_columns}
    for name, spec in resolve_columns(entry, common).items():
        column = actual.get(name.lower())
        if column is None:
            continue
        statements.append(build_column_comment_sql(table_fqn, column, spec["description"]))
        if spec.get("tags"):
            statements.append(build_column_tags_sql(table_fqn, column, spec["tags"]))
    return statements


def validate_catalog(catalog: dict) -> None:
    """Every dataset needs a description and every column a non-empty one."""
    problems = []
    for column, spec in catalog.get("common", {}).items():
        if not (spec or {}).get("description"):
            problems.append(f"_common.{column}: missing description")
    for dataset, entry in catalog.get("datasets", {}).items():
        if not entry.get("description"):
            problems.append(f"{dataset}: missing table description")
        for column, spec in entry.get("columns", {}).items():
            if not (spec or {}).get("description"):
                problems.append(f"{dataset}.{column}: missing description")
    if problems:
        raise ValueError("Invalid data catalog:\n  " + "\n  ".join(problems))


def _catalog_dir(path: str | None) -> Path:
    if path is not None:
        return Path(path)
    try:
        from importlib.resources import files

        packaged = files("dev_platform.governance").joinpath("data_catalog")
        if packaged.is_dir():
            return Path(str(packaged))
    except (ModuleNotFoundError, FileNotFoundError, AttributeError):
        pass
    # src/dev_platform/governance/catalog.py -> bundle root is 3 parents up.
    return Path(__file__).resolve().parents[3] / "conf" / "data_catalog"


def load_catalog(path: str | None = None) -> dict:
    """Read every YAML in the catalog directory into ``{"common": {...}, "datasets": {...}}``.

    Resolution order: an explicit ``path``; then the copy packaged in the wheel
    (``dev_platform/governance/data_catalog``); then ``conf/data_catalog`` in the repo.
    """
    import yaml  # lazy import: only needed when loading from YAML

    directory = _catalog_dir(path)
    catalog: dict = {"common": {}, "datasets": {}}
    for file in sorted(directory.glob("*.yml")):
        content = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
        if file.name == COMMON_FILE:
            catalog["common"] = content.get("columns", {})
        else:
            catalog["datasets"][content.get("dataset", file.stem)] = content
    validate_catalog(catalog)
    return catalog


def apply_table_metadata(
    spark: SparkSession,
    catalog: str,
    catalog_entries: dict,
    schema: str = "raw",
    datasets: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Describe (and tag the columns of) every catalog table that exists under ``<catalog>.<schema>``.

    Tables that don't exist yet are skipped. Returns ``{fully-qualified name: statements}``
    for the tables that were (or, with ``dry_run``, would be) updated.
    """
    entries = catalog_entries.get("datasets", {})
    names = datasets if datasets is not None else list(entries)
    applied: dict[str, int] = {}
    for name in names:
        fqn = f"{catalog}.{schema}.{name}"
        if name not in entries or not spark.catalog.tableExists(fqn):
            continue
        statements = plan_metadata(fqn, entries[name], spark.table(fqn).columns, catalog_entries.get("common"))
        if not dry_run:
            for statement in statements:
                spark.sql(statement)
        applied[fqn] = len(statements)
    return applied
