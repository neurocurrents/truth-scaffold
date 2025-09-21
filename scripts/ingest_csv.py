import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
dsn = os.environ.get("PG_DSN")
if not dsn:
    raise SystemExit("PG_DSN is not set. Put it in .env")

csv_path = os.getenv("CSV_PATH", "records.csv")  # default repo-root file
if not os.path.exists(csv_path):
    raise SystemExit(f"Missing {csv_path}. Place your cleaned CSV there or set CSV_PATH.")

print(f"Loading {csv_path} ...")
df = pd.read_csv(csv_path)

# Optional: basic dtype harmonization
if "pmid" in df.columns:
    df["pmid"] = pd.to_numeric(df["pmid"], errors="coerce").astype("Int64")

engine = create_engine(dsn, pool_pre_ping=True, future=True)

with engine.begin() as conn:
    # ensure the table exists
    conn.execute(text("SELECT 1 FROM records LIMIT 1"))
    # stage and upsert
    conn.execute(text("CREATE TEMP TABLE _stage (LIKE records INCLUDING ALL) ON COMMIT DROP;"))
    df.to_sql("_stage", conn.connection, if_exists="append", index=False)
    conn.execute(text("""        INSERT INTO records AS r
        SELECT * FROM _stage
        ON CONFLICT (pmid) DO UPDATE SET
          title=EXCLUDED.title,
          abstract=EXCLUDED.abstract,
          journal=EXCLUDED.journal,
          year=EXCLUDED.year,
          _hit_kw=EXCLUDED._hit_kw,
          demo_present=EXCLUDED.demo_present,
          outcome_present=EXCLUDED.outcome_present,
          is_1=EXCLUDED.is_1,
          decade=EXCLUDED.decade,
          pub_year_final=EXCLUDED.pub_year_final,
          is_paywalled=EXCLUDED.is_paywalled,
          years_since_pub=EXCLUDED.years_since_pub,
          is_pharma=EXCLUDED.is_pharma;
    """))

print("Ingest complete ✅")
