"""Migrate Price Marketing tables from data.db -> price.db.

Safe-by-default:
- Does NOT overwrite destination unless you pass --overwrite-dst
- Does NOT rename source tables unless you pass --rename-source

Usage:
  python migrate_price_data.py
  python migrate_price_data.py --overwrite-dst
  python migrate_price_data.py --overwrite-dst --rename-source

Notes:
- Expects both DB files to live next to this script for local dev.
- On Railway Volume, uses RAILWAY_VOLUME_MOUNT_PATH like app.py.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime

TABLES = [
    # dependency order: items before logs (logs references market_item_id)
    "platform_fee_settings",
    "sku_pricing",
    "market_items",
    "market_price_logs",
]


def _db_paths() -> tuple[str, str]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    volume_path = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    if volume_path:
        return os.path.join(volume_path, "data.db"), os.path.join(volume_path, "price.db")
    return os.path.join(base_dir, "data.db"), os.path.join(base_dir, "price.db")


def _table_exists(cur: sqlite3.Cursor, schema: str, table: str) -> bool:
    cur.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    )
    return cur.fetchone() is not None


def _row_count(cur: sqlite3.Cursor, schema: str, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--overwrite-dst",
        action="store_true",
        help="Delete destination tables then copy everything from source.",
    )
    parser.add_argument(
        "--rename-source",
        action="store_true",
        help="Rename source tables in data.db after successful copy (to reduce confusion).",
    )
    args = parser.parse_args()

    src_path, dst_path = _db_paths()

    if not os.path.exists(src_path):
        print(f"❌ source not found: {src_path}")
        return 2

    if not os.path.exists(dst_path):
        print(f"❌ destination not found: {dst_path}")
        print("Run init_price_db.py once to create price.db + tables.")
        return 2

    con = sqlite3.connect(src_path)
    try:
        cur = con.cursor()
        cur.execute(f"ATTACH DATABASE '{dst_path}' AS price")

        copied_any = False
        for table in TABLES:
            if not _table_exists(cur, "main", table):
                print(f"- skip (missing in data.db): {table}")
                continue
            if not _table_exists(cur, "price", table):
                print(f"❌ missing in price.db (run init_price_db.py): {table}")
                return 2

            src_count = _row_count(cur, "main", table)
            dst_count = _row_count(cur, "price", table)

            if dst_count > 0 and not args.overwrite_dst:
                print(
                    f"- skip (dst has data, use --overwrite-dst): {table} (src={src_count}, dst={dst_count})"
                )
                continue

            if args.overwrite_dst:
                cur.execute(f"DELETE FROM price.{table}")

            cur.execute(f"INSERT INTO price.{table} SELECT * FROM main.{table}")
            new_dst_count = _row_count(cur, "price", table)
            print(f"- copied: {table} (src={src_count}, dst={new_dst_count})")
            copied_any = True

        if copied_any:
            con.commit()
            print("✅ migrate done")
        else:
            print("ℹ️ nothing to migrate")

        if args.rename_source:
            suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            for table in TABLES:
                if not _table_exists(cur, "main", table):
                    continue
                legacy_name = f"{table}__legacy__{suffix}"
                # Avoid renaming if it somehow already exists
                if _table_exists(cur, "main", legacy_name):
                    print(f"- rename skip (already exists): {legacy_name}")
                    continue
                cur.execute(f"ALTER TABLE main.{table} RENAME TO {legacy_name}")
                print(f"- renamed source table: {table} -> {legacy_name}")
            con.commit()
            print("✅ renamed source tables")

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
