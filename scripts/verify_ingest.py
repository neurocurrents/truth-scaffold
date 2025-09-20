import argparse, os, sys, csv, math, hashlib
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def infer_datetime_cols(df: pd.DataFrame):
    dt_cols = []
    for col in df.columns:
        s = df[col].dropna().astype(str).head(200)  # sample
        hits = 0
        for v in s:
            try:
                _ = pd.to_datetime(v, errors="raise", utc=False, infer_datetime_format=True)
                hits += 1
            except Exception:
                pass
        if len(s) > 0 and hits / len(s) > 0.8:
            dt_cols.append(col)
    return dt_cols

def main():
    ap = argparse.ArgumentParser(description="Verify CSV-to-Postgres ingest")
    ap.add_argument("--csv", required=True, help="Path to source CSV")
    ap.add_argument("--table", required=True, help="Destination table (schema.table or table)")
    ap.add_argument("--pg-dsn", default=os.getenv("PG_DSN"), help="Postgres DSN (or set PG_DSN env)")
    ap.add_argument("--key-cols", nargs="*", help="Optional list of columns that form a unique key")
    ap.add_argument("--sample", type=int, default=1000, help="Rows to sample from CSV for key check")
    args = ap.parse_args()

    if not args.pg_dsn:
        print("PG_DSN not provided. Use --pg-dsn or set PG_DSN env.", file=sys.stderr)
        sys.exit(2)

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    # Load just the header first
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    csv_cols = [h.strip() for h in header]

    # Count rows quickly without loading whole file to memory
    csv_rows = 0
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        for _ in f:
            csv_rows += 1
    csv_rows -= 1  # minus header

    print(f"[CSV] columns: {len(csv_cols)} | rows: {csv_rows}")
    print(f"[CSV] first 5 columns: {csv_cols[:5]}")

    # Make a typed sample dataframe for profiling (fast enough)
    df_sample = pd.read_csv(csv_path, nrows=min(100000, max(2000, args.sample)), low_memory=False)
    # Best-effort parse numerics
    for c in df_sample.columns:
        df_sample[c] = pd.to_numeric(df_sample[c], errors="ignore")
    dt_cols = infer_datetime_cols(df_sample)

    engine = create_engine(args.pg_dsn, pool_pre_ping=True, future=True)
    schema, table = None, None
    if "." in args.table:
        schema, table = args.table.split(".", 1)
    else:
        table = args.table

    # Pull destination metadata
    with engine.begin() as conn:
        # Row count
        qcount = f'SELECT COUNT(*) FROM {args.table};'
        dest_rows = conn.execute(text(qcount)).scalar_one()
        print(f"[DB ] rows: {dest_rows}")

        # Columns (ordered)
        if schema:
            meta_sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position;
            """
            dest_cols_types = conn.execute(text(meta_sql), {"schema": schema, "table": table}).all()
        else:
            meta_sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :table
            ORDER BY ordinal_position;
            """
            dest_cols_types = conn.execute(text(meta_sql), {"table": table}).all()

    dest_cols = [r[0] for r in dest_cols_types]
    print(f"[DB ] columns: {len(dest_cols)}")
    print(f"[DB ] first 5 columns: {dest_cols[:5]}")

    # 1) Column comparison
    missing_in_db = [c for c in csv_cols if c not in dest_cols]
    extra_in_db   = [c for c in dest_cols if c not in csv_cols]
    if missing_in_db:
        print(f"[WARN] Columns present in CSV but missing in DB: {missing_in_db}")
    if extra_in_db:
        print(f"[INFO] Columns present in DB but not in CSV: {extra_in_db}")

    # 2) Row count comparison
    if int(dest_rows) != int(csv_rows):
        print(f"[FAIL] Row count mismatch: CSV={csv_rows} vs DB={dest_rows}")
    else:
        print(f"[OK] Row counts match.")

    # 3) Per-column null counts (DB side) & sanity ranges
    with engine.begin() as conn:
        for col in csv_cols:
            if col not in dest_cols:
                continue
            # Null count
            null_sql = f"SELECT COUNT(*) FROM {args.table} WHERE {col} IS NULL;"
            nulls = conn.execute(text(null_sql)).scalar_one()

            # Simple min/max for numeric or datelike columns
            min_val = max_val = None
            if col in df_sample.select_dtypes(include=["number"]).columns:
                min_val = conn.execute(text(f"SELECT MIN({col}) FROM {args.table};")).scalar_one()
                max_val = conn.execute(text(f"SELECT MAX({col}) FROM {args.table};")).scalar_one()
            elif col in dt_cols:
                min_val = conn.execute(text(f"SELECT MIN({col}::timestamp) FROM {args.table} WHERE {col} IS NOT NULL;")).scalar_one()
                max_val = conn.execute(text(f"SELECT MAX({col}::timestamp) FROM {args.table} WHERE {col} IS NOT NULL;")).scalar_one()

            print(f"[DB ] {col}: nulls={nulls}", end="")
            if min_val is not None or max_val is not None:
                print(f" | min={min_val} max={max_val}")
            else:
                print()

    # 4) Optional exactness: existence check for a sample via key columns
    if args.key_cols:
        keys = [k for k in args.key_cols if k in df_sample.columns and k in dest_cols]
        if not keys:
            print(f"[WARN] None of the provided key columns are present in both CSV and DB.")
        else:
            print(f"[Check] Using key columns: {keys}")
            csv_small = pd.read_csv(csv_path, usecols=keys, nrows=args.sample)
            csv_small = csv_small.dropna(subset=keys)
            placeholders = " AND ".join([f"{k} = :{k}" for k in keys])

            missing = 0
            with engine.begin() as conn:
                for _, row in csv_small.iterrows():
                    params = {k: (None if (isinstance(row[k], float) and math.isnan(row[k])) else row[k]) for k in keys}
                    sql = f"SELECT 1 FROM {args.table} WHERE {placeholders} LIMIT 1;"
                    hit = conn.execute(text(sql), params).fetchone()
                    if hit is None:
                        missing += 1
                if missing == 0:
                    print(f"[OK] Sampled {len(csv_small)} CSV rows found in DB by key.")
                else:
                    print(f"[FAIL] {missing} of {len(csv_small)} sampled rows not found by key.")

if __name__ == "__main__":
    main()
