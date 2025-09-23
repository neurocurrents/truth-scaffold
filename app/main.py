# app/main.py
import os
from contextlib import asynccontextmanager
from typing import Dict, List

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text

# ----------------- helpers -----------------
def _load_dsn() -> str:
    load_dotenv()
    dsn = os.getenv("PG_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Database DSN missing.")
    if dsn.startswith("postgres://"):
        dsn = "postgresql+psycopg://" + dsn[len("postgres://"):]
    elif dsn.startswith("postgresql://") and "+psycopg" not in dsn and "+psycopg2" not in dsn:
        dsn = "postgresql+psycopg://" + dsn[len("postgresql://"):]
    dsn = dsn.replace("postgresql+psycopg2://", "postgresql+psycopg://")
    return dsn




@asynccontextmanager
async def lifespan(app: FastAPI):
    dsn = _load_dsn()
    app.state.engine = create_engine(dsn, pool_pre_ping=True, future=True)
    try:
        yield
    finally:
        app.state.engine.dispose()


# ----------------- app setup -----------------
app = FastAPI(title="Truth Scaffold API", lifespan=lifespan)

# CORS (tighten this later to your front-end domains)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fts_exists(conn) -> bool:
    return bool(
        conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='records'
                  AND column_name='fts'
                """
            )
        ).first()
    )


# prefer integer compliance; else map boolean is_compliant → 0/1
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


# ----------------- routes -----------------
@app.get("/healthz")
def healthz(request: Request) -> Dict:
    with request.app.state.engine.begin() as c:
        one = c.execute(text("SELECT 1")).scalar_one()
    return {"ok": True, "db": one}


@app.get("/columns")
def columns(request: Request) -> List[Dict]:
    with request.app.state.engine.begin() as c:
        rows = c.execute(
            text(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='records'
                ORDER BY ordinal_position
                """
            )
        ).mappings().all()
    return [dict(r) for r in rows]


@app.get("/counts")
def counts(request: Request) -> Dict:
    sql = f"""
        SELECT
          COUNT(*)                                   AS total,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR}=1) AS compliant_1,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR}=0) AS compliant_0,
          COUNT(*) FILTER (WHERE {COMPLIANCE_EXPR} IS NULL) AS compliant_nulls
        FROM public.records
    """
    with request.app.state.engine.begin() as c:
        row = c.execute(text(sql)).mappings().one()
    total = row["total"] or 0
    return {
        **row,
        "compliance_rate": (row["compliant_1"] / total) if total else 0.0,
    }


@app.get("/summary/decade")
def summary_by_decade(request: Request) -> List[Dict]:
    sql = f"""
        SELECT decade,
               COUNT(*) AS n,
               AVG(({COMPLIANCE_EXPR})::numeric)::float AS compliance_rate
        FROM public.records
        GROUP BY decade
        ORDER BY decade
    """
    with request.app.state.engine.begin() as c:
        rows = c.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]


@app.get("/articles/search")
def search(
    request: Request, q: str = Query(..., min_length=2), limit: int = 20
) -> List[Dict]:
    q = " ".join(q.split())
    with request.app.state.engine.begin() as c:
        if fts_exists(c):
            ts = " & ".join("".join(ch if ch.isalnum() else " " for ch in q).split())
            rows = c.execute(
                text(
                    """
                    SELECT pmid, title, journal, decade
                    FROM public.records
                    WHERE fts @@ to_tsquery('english', :ts)
                    ORDER BY pmid
                    LIMIT :limit
                    """
                ),
                {"ts": ts, "limit": limit},
            ).mappings().all()
        else:
            rows = c.execute(
                text(
                    """
                    SELECT pmid, title, journal, decade
                    FROM public.records
                    WHERE (title ILIKE :pat OR abstract ILIKE :pat)
                    ORDER BY pmid
                    LIMIT :limit
                    """
                ),
                {"pat": f"%{q}%", "limit": limit},
            ).mappings().all()
    return [dict(r) for r in rows]
