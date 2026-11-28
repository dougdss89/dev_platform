"""Entrypoint — apply governance metadata (tags, descriptions, column tags) to materialized tables.

Idempotent governance task: run it after the layer jobs, or any time the catalog files change.
For each schema it applies the table tags from ``conf/data_tags.yml`` and the descriptions and
column tags from ``conf/data_catalog/``, only on tables that already exist.

Invoked as a ``python_wheel_task`` with positional parameters:
    [catalog, schemas]   # schemas optional, comma separated, defaults to "raw,trusted"
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.governance.catalog import apply_table_metadata, load_catalog
from dev_platform.governance.tagging import apply_table_tags, load_taxonomy


def main() -> None:
    catalog = sys.argv[1]
    schemas = sys.argv[2].split(",") if len(sys.argv) > 2 else ["raw", "trusted"]
    spark = SparkSession.builder.getOrCreate()
    taxonomy, data_catalog = load_taxonomy(), load_catalog()
    for schema in schemas:
        tagged = apply_table_tags(spark, catalog, taxonomy, schema=schema)
        described = apply_table_metadata(spark, catalog, data_catalog, schema=schema)
        print(f"{catalog}.{schema}: tagged {len(tagged)} tables, described {len(described)}: {described}")


if __name__ == "__main__":
    main()
