"""Entrypoint (composition root) — buyer raw -> trusted.

Same design as buyer_ingest: thin driver, zero business logic. Only changes
which factory is wired. DABs invokes this entry point after landing -> raw (depends_on).
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.domains.buyer.buyer_pipeline import build_buyer_trusted


def main() -> None:
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    build_buyer_trusted(spark, catalog).run()


if __name__ == "__main__":
    main()
