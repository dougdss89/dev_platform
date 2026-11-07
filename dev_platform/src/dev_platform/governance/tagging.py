"""Apply the data-governance taxonomy as Unity Catalog tags.

The taxonomy in ``conf/data_tags.yml`` is the single source of truth. These
helpers turn each dataset entry into ``ALTER TABLE ... SET TAGS`` on the
materialized UC table.

The pure helpers (``resolve_tags``, ``build_set_tags_sql``) take a plain dict, so
they are unit-testable without Spark. ``apply_table_tags`` runs them against a
SparkSession; ``load_taxonomy`` reads the YAML (needs PyYAML, which the Databricks
runtime provides).
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def _tag_value(value: object) -> str:
    """UC tag values are strings: normalize booleans and escape single quotes."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).replace("'", "''")


def resolve_tags(dataset: str, taxonomy: dict, layer: str | None = None) -> dict:
    """Merge the taxonomy defaults with a dataset's entry (the entry wins).

    ``layer`` overrides the default layer tag, so applying to the trusted schema
    stamps ``layer=trusted`` even though the taxonomy default is ``raw``.
    """
    entry = taxonomy.get("datasets", {}).get(dataset)
    if entry is None:
        raise KeyError(f"dataset '{dataset}' not found in taxonomy")
    tags = {**taxonomy.get("defaults", {}), **entry}
    if layer is not None:
        tags["layer"] = layer
    return tags


def build_set_tags_sql(table_fqn: str, tags: dict) -> str:
    """Build ``ALTER TABLE <fqn> SET TAGS ('k' = 'v', ...)``."""
    pairs = ", ".join(f"'{k}' = '{_tag_value(v)}'" for k, v in tags.items())
    return f"ALTER TABLE {table_fqn} SET TAGS ({pairs})"


def apply_table_tags(
    spark: SparkSession,
    catalog: str,
    taxonomy: dict,
    schema: str = "raw",
    datasets: list[str] | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Tag every taxonomy table that exists under ``<catalog>.<schema>``.

    Tables that don't exist yet are skipped. ``layer`` is set to ``schema``.
    Returns the fully-qualified names that were (or, with ``dry_run``, would be) tagged.
    """
    names = datasets if datasets is not None else list(taxonomy.get("datasets", {}))
    applied: list[str] = []
    for name in names:
        fqn = f"{catalog}.{schema}.{name}"
        if not spark.catalog.tableExists(fqn):
            continue
        sql = build_set_tags_sql(fqn, resolve_tags(name, taxonomy, layer=schema))
        if not dry_run:
            spark.sql(sql)
        applied.append(fqn)
    return applied


def load_taxonomy(path: str | None = None) -> dict:
    """Load the taxonomy YAML.

    Resolution order: an explicit ``path``; then the copy packaged in the wheel
    (``dev_platform/governance/data_tags.yml``); then ``conf/data_tags.yml``
    relative to the repo (handy in local development).
    """
    import yaml  # lazy import: only needed when loading from YAML

    if path is not None:
        with open(path, encoding="utf-8") as fh:
            return yaml.safe_load(fh)

    try:
        from importlib.resources import files

        packaged = files("dev_platform.governance").joinpath("data_tags.yml")
        if packaged.is_file():
            return yaml.safe_load(packaged.read_text(encoding="utf-8"))
    except (ModuleNotFoundError, FileNotFoundError, AttributeError):
        pass

    from pathlib import Path

    # src/dev_platform/governance/tagging.py -> bundle root is 3 parents up.
    repo_copy = Path(__file__).resolve().parents[3] / "conf" / "data_tags.yml"
    with open(repo_copy, encoding="utf-8") as fh:
        return yaml.safe_load(fh)
