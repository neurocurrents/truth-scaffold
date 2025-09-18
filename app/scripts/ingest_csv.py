import argparse
import os
import pandas as pd
from sqlalchemy import create_engine, text

PG_DSN = os.getenv("PG_DSN")
engine = create_engine(PG_DSN)

def upsert_postgres(df: pd.DataFrame):
    if "decade" not in df.columns:
        df["decade"] = (pd.to_numeric(df.get("year"), errors="coerce") // 10 * 10).astype("Int64")
    with engine.begin() as conn:
        df.to_sql("records", conn, if_exists="append", index=False, method="multi", chunksize=1000)
        conn.execute(text(
            "UPDATE records SET fts = to_tsvector('english', coalesce(title,'') || ' ' || coalesce(abstract,''));"
        ))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to records.csv (schema as in SQL table)")
    args = ap.parse_args()
    df = pd.read_csv(args.csv)
    upsert_postgres(df)
    print(f"Ingested {len(df)} records → Postgres (FTS ready)")

if __name__ == "__main__":
    main()
