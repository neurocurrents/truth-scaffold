import os
from typing import Optional
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # or restrict to your preview URL(s)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()
engine = create_engine(os.environ["PG_DSN"], pool_pre_ping=True, future=True)
app = FastAPI(title="Truth Scaffold API")

def has_fts(conn) -> bool:
    return bool(conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='records' AND column_name='fts'
    """)).first())

@app.get("/health")
def health():
    with engine.begin() as c:
        v = c.execute(text("SELECT 1")).scalar_one()
    return {"ok": True, "db": v}

@app.get("/counts")
def counts():
    with engine.begin() as c:
        total = c.execute(text("SELECT COUNT(*) FROM records")).scalar_one()
        compliant = c.execute(text("SELECT COUNT(*) FROM records WHERE is_compliant IS TRUE")).scalar_one()
    rate = round(compliant/total, 4) if total else 0.0
    return {"total": total, "compliant": compliant, "compliance_rate": rate}

@app.get("/summary/decade")
def summary_by_decade():
    sql = """
    SELECT decade, COUNT(*) AS n,
           AVG((is_compliant)::int)::float AS compliance_rate
    FROM records
    GROUP BY decade
    ORDER BY decade
    """
    with engine.begin() as c:
        rows = [dict(r._mapping) for r in c.execute(text(sql))]
    return rows

@app.get("/summary/study-type")
def summary_by_study_type():
    sql = """
    SELECT study_type, COUNT(*) AS n,
           AVG((is_compliant)::int)::float AS compliance_rate
    FROM records
    GROUP BY study_type
    ORDER BY n DESC
    """
    with engine.begin() as c:
        rows = [dict(r._mapping) for r in c.execute(text(sql))]
    return rows

@app.get("/summary/matrix")
def matrix_decade_x_study():
    sql = """
    SELECT decade, study_type, COUNT(*) AS n,
           AVG((is_compliant)::int)::float AS compliance_rate
    FROM records
    GROUP BY decade, study_type
    ORDER BY decade, n DESC
    """
    with engine.begin() as c:
        rows = [dict(r._mapping) for r in c.execute(text(sql))]
    return rows

@app.get("/outcomes/polarity")
def outcomes_polarity():
    sql = """
    SELECT outcome_sign, COUNT(*) AS n
    FROM records
    GROUP BY outcome_sign
    ORDER BY n DESC
    """
    with engine.begin() as c:
        rows = [dict(r._mapping) for r in c.execute(text(sql))]
    return rows

@app.get("/articles/search")
def search(q: str = Query(..., min_length=2), limit: int = 20):
    q = " ".join(q.split())  # normalize spaces
    with engine.begin() as c:
        if has_fts(c):
            # basic tsquery sanitization: replace non-word with & and dedupe &
            ts = " & ".join(filter(None, ["".join(ch if ch.isalnum() else " " for ch in q).strip().replace("  "," " ).replace(" "," & ")]))
            rows = c.execute(text("""
                SELECT pmid, title, journal, decade
                FROM records
                WHERE fts @@ to_tsquery('english', :ts)
                ORDER BY pmid
                LIMIT :limit
            """), {"ts": ts, "limit": limit}).mappings().all()
        else:
            rows = c.execute(text("""
                SELECT pmid, title, journal, decade
                FROM records
                WHERE (title ILIKE :pat OR abstract ILIKE :pat)
                ORDER BY pmid
                LIMIT :limit
            """), {"pat": f"%{q}%", "limit": limit}).mappings().all()
    return [dict(r) for r in rows]
