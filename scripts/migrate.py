import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
dsn = os.environ.get("PG_DSN")
if not dsn:
    raise SystemExit("PG_DSN is not set. Put it in .env")

engine = create_engine(dsn, pool_pre_ping=True, future=True)

DDL = """CREATE EXTENSION IF NOT EXISTS vector;  -- safe if pgvector is available
CREATE TABLE IF NOT EXISTS records (
  pmid bigint PRIMARY KEY,
  title text,
  abstract text,
  journal text,
  year double precision,
  _hit_kw text,
  demo_present double precision,
  outcome_present double precision,
  is_1 double precision,
  decade double precision,
  pub_year_final double precision,
  is_paywalled double precision,
  years_since_pub double precision,
  is_pharma integer
);
"""

with engine.begin() as conn:
    for stmt in DDL.strip().split(";"):
        s = stmt.strip()
        if s:
            conn.execute(text(s))

print("Migration complete ✅")
