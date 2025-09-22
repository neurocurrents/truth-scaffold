# app/main.py
import os
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_DSN = os.getenv("PG_DSN")
if not PG_DSN:
    raise RuntimeError("PG_DSN is not set. Add it to your Render env vars and .env locally.")

engine = create_engine(PG_DSN, pool_pre_ping=True, future=True)

app = FastAPI(title="Truth Scaffold API")

# CORS (adjust allow_origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def col_exists(conn, name: str) -> bool:
    return bool(
        conn.execute(
            text("""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='records'
                  AND column_name=:name
            """),
            {"name": name},
        ).first()
    )

def fts_exists(conn) -> bool:
    return col_exists(conn, "fts")

# Use a unified expression: prefer integer `compliance`; else map boolean `is_compliant` → 0/1.
COMPLIANCE_EXPR = """
COALESCE(
  compliance,
  CASE
    WHEN is_compliant IS TRUE  THEN 1
    WHEN is_compliant IS FALSE THEN 0
    ELSE NULL
  END
)
"""

@app.get("/healthz")
def healthz():
    with engine.begin() as c:
        one = c.execute(text("SELECT 1")).scalar_one()
    return {"ok": True, "db": one}

@app.get("/columns")
def columns():
    with engine.begin() as c:
        rows = c.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='records'
            ORDER BY ordinal_position
        """)).mappings().all()
    return [dict(r) for r in rows]

@app.get("/counts")
def counts():
    sql = f"""
        SELECT
          COUNT(*)                                   AS total,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR}=1) AS compliant_1,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR}=0) AS compliant_0,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR} IS NULL) AS compliant_nulls
        FROM public.records
    """
    with engine.begin() as c:
        row = c.execute(text(sql)).mappings().one()
    # add rate
    total = row["total"] or 0
    row = dict(row)
    row["compliance_rate"] = (row["compliant_1"] / total) if total else 0.0
    return row

@app.get("/summary/decade")
def summary_by_decade():
    sql = f"""
        SELECT decade,
               COUNT(*) AS n,
               AVG(({COMPLIANCE_EXPR})::numeric)::float AS compliance_rate
        FROM public.records
        GROUP BY decade
        ORDER BY decade
    """
    with engine.begin() as c:
        rows = c.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]

@app.get("/articles/search")
def search(q: str = Query(..., min_length=2), limit: int = 20):
    q = " ".join(q.split())
    with engine.begin() as c:
        if fts_exists(c):
            # create a conservative tsquery from q
            ts = " & ".join("".join(ch if ch.isalnum() else " " for ch in q).split())
            rows = c.execute(
                text("""
                    SELECT pmid, title, journal, decade
                    FROM public.records
                    WHERE fts @@ to_tsquery('english', :ts)
                    ORDER BY pmid
                    LIMIT :limit
                """),
                {"ts": ts, "limit": limit},
            ).mappings().all()
        else:
            rows = c.execute(
                text("""
                    SELECT pmid, title, journal, decade
                    FROM public.records
                    WHERE (title ILIKE :pat OR abstract ILIKE :pat)
                    ORDER BY pmid
                    LIMIT :limit
                """),
                {"pat": f"%{q}%", "limit": limit},
            ).mappings().all()
    return [dict(r) for r in rows]
