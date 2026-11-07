#!/usr/bin/env python3
"""Seed small batches of synthetic `customer` rows into the landing volume.

Writes one tiny parquet file per batch to the UC volume that mirrors S3
(example.raw.landing_data/customer), matching the EXISTING schema so the raw
ingestion picks it up unchanged. Kept intentionally small to avoid cost.

Run (needs pyarrow — the connect venv has it):
    connect/.venv/bin/python scripts/seed_landing_customer.py \
        --profile community --batch-size 8 --interval 60 --iterations 10

Flags:
    --batch-size   rows per file        (default 8)
    --interval     seconds between files (default 60)
    --iterations   number of files; 0 = run until Ctrl+C (default 10)
    --profile      Databricks CLI profile (default community)
    --volume       target dir           (default the customer landing path)

Seeded customerids start at 1_000_000 so they never collide with real ids
(they land as clean inserts for the trusted MERGE).
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import subprocess
import tempfile
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Exact schema of the existing landing parquet — order and types must match.
SCHEMA = pa.schema(
    [
        ("businessentityid", pa.int32()),
        ("customerid", pa.int32()),
        ("firstname", pa.string()),
        ("lastname", pa.string()),
        ("persontype", pa.string()),
        ("emailpromotion", pa.int32()),
        ("typephone", pa.string()),
        ("phonenumber", pa.string()),
        ("postalcode", pa.string()),
        ("addresstype", pa.string()),
        ("emailaddress", pa.string()),
        ("territoryid", pa.int32()),
        ("CountryRegionName", pa.string()),
        ("stateprovinceid", pa.int32()),
        ("countryregioncode", pa.string()),
        ("stateprovincecode", pa.string()),
        ("statename", pa.string()),
        ("city", pa.string()),
        ("addressline1", pa.string()),
        ("addressline2", pa.string()),
        ("TotalPurchaseYTD", pa.decimal128(12, 2)),
        ("DateFirstPurchase", pa.date32()),
        ("BirthDate", pa.date32()),
        ("MaritalStatus", pa.string()),
        ("YearlyIncome", pa.string()),
        ("Gender", pa.string()),
        ("TotalChildren", pa.int32()),
        ("NumberChildrenAtHome", pa.int32()),
        ("Education", pa.string()),
        ("Occupation", pa.string()),
        ("HomeOwnerFlag", pa.string()),
        ("NumberCarsOwned", pa.int32()),
        ("inserted_date", pa.timestamp("us")),  # us, not ns: Spark can't read NANOS parquet timestamps
        ("modified_date", pa.string()),
        ("eltversion", pa.string()),
    ]
)

FIRST = ["Calvin", "Maria", "John", "Ana", "Liu", "Omar", "Sophie", "Diego", "Ingrid", "Noah"]
LAST = ["Raji", "Silva", "Smith", "Costa", "Wang", "Hassan", "Martin", "Lopez", "Berg", "Khan"]
STATES = [("WA ", "Washington", "Bothell"), ("CA ", "California", "San Diego"), ("OR ", "Oregon", "Portland")]
STREETS = ["San Gabriel Dr.", "Main St.", "Oak Rd.", "Elm Blvd.", "Lincoln Ave.", "Pine Ct."]
INCOME = ["0-25000", "25001-50000", "50001-75000", "75001-100000", "100001-150000"]
EDU = ["High School ", "Bachelors ", "Graduate Degree ", "Partial College "]
OCC = ["Professional", "Management", "Skilled Manual", "Clerical", "Manual"]


def _row(cid: int) -> dict:
    first, last = random.choice(FIRST), random.choice(LAST)
    code, name, city = random.choice(STATES)
    return {
        "businessentityid": cid,
        "customerid": cid,
        "firstname": first,
        "lastname": last,
        "persontype": "IN",
        "emailpromotion": random.choice([0, 1, 2]),
        "typephone": random.choice(["Cell", "Home", "Work"]),
        "phonenumber": f"{random.randint(200, 999)}-555-{random.randint(1000, 9999)}",
        "postalcode": f"{random.randint(10000, 99999)}",
        "addresstype": random.choice(["Shipping", "Main Office", "Home"]),
        "emailaddress": f"{first.lower()}{random.randint(1, 99)}@adventure-works.com",
        "territoryid": random.randint(1, 10),
        "CountryRegionName": "United States",
        "stateprovinceid": random.randint(1, 80),
        "countryregioncode": "US",
        "stateprovincecode": code,
        "statename": name,
        "city": city,
        "addressline1": f"{random.randint(100, 9999)} {random.choice(STREETS)}",
        "addressline2": None,
        "TotalPurchaseYTD": Decimal(f"{random.randint(-500, 5000)}.{random.randint(0, 99):02d}"),
        "DateFirstPurchase": dt.date(random.randint(2001, 2008), random.randint(1, 12), random.randint(1, 28)),
        "BirthDate": dt.date(random.randint(1950, 1995), random.randint(1, 12), random.randint(1, 28)),
        "MaritalStatus": random.choice(["M", "S"]),
        "YearlyIncome": random.choice(INCOME),
        "Gender": random.choice(["M", "F"]),
        "TotalChildren": random.randint(0, 5),
        "NumberChildrenAtHome": random.randint(0, 3),
        "Education": random.choice(EDU),
        "Occupation": random.choice(OCC),
        "HomeOwnerFlag": random.choice(["0", "1"]),
        "NumberCarsOwned": random.randint(0, 4),
        "inserted_date": dt.datetime.now(),
        "modified_date": "9999-31-12 00:00:00.000",
        "eltversion": "1",
    }


def _batch_table(n: int, base_id: int) -> pa.Table:
    rows = [_row(base_id + i) for i in range(n)]
    columns = {name: [r[name] for r in rows] for name in SCHEMA.names}
    return pa.table(columns, schema=SCHEMA)


def _upload(table: pa.Table, volume: str, profile: str) -> str:
    fname = f"seed-{dt.datetime.now():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / fname
        pq.write_table(table, local, compression="snappy")
        remote = f"{volume.rstrip('/')}/{fname}"
        subprocess.run(
            ["databricks", "fs", "cp", str(local), remote, "--profile", profile],
            check=True,
        )
    return fname


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--batch-size", type=int, default=8)
    ap.add_argument("--interval", type=float, default=60.0)
    ap.add_argument("--iterations", type=int, default=10, help="0 = run until Ctrl+C")
    ap.add_argument("--profile", default="community")
    ap.add_argument("--volume", default="dbfs:/Volumes/example/raw/landing_data/customer")
    ap.add_argument("--base-id", type=int, default=1_000_000, help="starting customerid (avoids real-id collisions)")
    args = ap.parse_args()

    next_id = args.base_id
    written = 0
    try:
        while args.iterations == 0 or written < args.iterations:
            table = _batch_table(args.batch_size, next_id)
            fname = _upload(table, args.volume, args.profile)
            next_id += args.batch_size
            written += 1
            print(f"[{written}] wrote {args.batch_size} rows -> {fname}")
            if args.iterations != 0 and written >= args.iterations:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nstopped — {written} files written")


if __name__ == "__main__":
    main()
