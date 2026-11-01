"""Entrypoint (composition root) — customer raw -> trusted.

Same design as customer_ingest: thin driver, zero business logic. Only changes
which factory is wired. DABs invokes this entry point after landing -> raw (depends_on).
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.domains.customer.pipeline import build_customer_trusted


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    build_customer_trusted(spark, catalog).run()


if __name__ == "__main__":
    main()
