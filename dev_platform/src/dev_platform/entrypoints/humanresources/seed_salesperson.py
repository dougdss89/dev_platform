"""Entrypoint — maybe seed one small salesperson batch into the landing volume.

One job run = at most one batch; whether it writes is random, so some runs load
nothing. Invoked as a python_wheel_task with positional parameters:
    [landing_catalog, max_batch_size, load_probability]   # all optional: "example", 3, 0.5
"""

import sys

from pyspark.sql import SparkSession

from dev_platform.seeding.salesperson_seed import seed_once


def main() -> None:
    landing_catalog = sys.argv[1] if len(sys.argv) > 1 else "example"
    max_batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    load_probability = float(sys.argv[3]) if len(sys.argv) > 3 else 0.5
    spark = SparkSession.builder.getOrCreate()
    volume_path = f"/Volumes/{landing_catalog}/raw/landing_data/salesperson"
    result = seed_once(spark, volume_path, max_batch_size, load_probability)
    if result is None:
        print(f"No load this run (load_probability={load_probability})")
        return
    base_id, n = result
    print(f"Seeded {n} rows into {volume_path} (base_id={base_id})")


if __name__ == "__main__":
    main()
