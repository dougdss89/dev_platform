"""Entrypoint — apply governance tags to a layer's materialized tables.

Occasional governance task (not per-run): reads the taxonomy and runs
``ALTER TABLE ... SET TAGS`` on every table that exists under the given schema.

Invoked as a ``python_wheel_task`` with positional parameters:
    [catalog, schema]   # schema optional, defaults to "raw"
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.governance.tagging import apply_table_tags, load_taxonomy


def main() -> None:
    catalog = sys.argv[1]
    schema = sys.argv[2] if len(sys.argv) > 2 else "raw"
    spark = SparkSession.builder.getOrCreate()
    applied = apply_table_tags(spark, catalog, load_taxonomy(), schema=schema)
    print(f"Tagged {len(applied)} tables in {catalog}.{schema}: {applied}")


if __name__ == "__main__":
    main()
