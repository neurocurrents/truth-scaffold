#!/usr/bin/env python
import argparse, os, sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

def main():
    ap = argparse.ArgumentParser(description="CSV → Postgres ingest")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--table", required=True, help="Destination table (schema.table or table)")
    ap.add_argument("--truncate", action="store_true", help="TRUNCATE destination before ingest")
    ap.add_argument("--pg-dsn", default=os.getenv("PG_DSN"), help="Postgres DSN (or set PG_DSN)")
    ap.add_argument("--chunksize", type=int, default=5000, help="Bulk insert chunk size")
    args = ap.parse_args()

    if not args.pg_dsn:
        sys.exit("PG_DSN is not set")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        sys.exit(f"CSV not found: {csv_path}")

    # Load CSV
    df = pd.read_csv(csv_path, low_memory=False)

    # Ensure compliance is Int64 (0/1/null)
    if "compliance" in df.columns:
        df["compliance"] = pd.to_numeric(df["compliance"], errors="coerce").astype("Int64")

    eng = create_engine(args.pg_dsn, pool_pre_ping=True, future=True)
    schema, table = (args.table.split(".",1)+[None])[:2] if "." in args.table else (None, args.table)

    with eng.begin() as c:
        if args.truncate:
            c.execute(text(f"TRUNCATE TABLE {args.table};"))

    df.to_sql(
        name=table if schema is None else table,
        con=eng,
        schema=None if schema is None else schema,
        if_exists="append",
        index=False,
        chunksize=args.chunksize,
        method="multi"
    )

    with eng.begin() as c:
        total = c.execute(text(f"SELECT COUNT(*) FROM {args.table}")).scalar_one()
        comp = c.execute(text(f"""
            SELECT compliance, COUNT(*) 
            FROM {args.table}
            GROUP BY compliance ORDER BY compliance
        """)).all()
    print(f"[DONE] wrote {len(df):,} rows → {args.table}")
    print(f"[INFO] total rows in table: {total}")
    print(f"[INFO] compliance breakdown: {dict(comp)}")

if __name__ == "__main__":
    main()
