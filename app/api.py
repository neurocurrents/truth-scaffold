from fastapi import FastAPI, Query
from .search import fts_search
from .db import fetch_paper

app = FastAPI(title="Truth Scaffold API", version="0.1.0")

@app.get("/search")
def search(q: str = "", domain: str | None = None, compliance: str | None = None, replication: str | None = None,
           year_min: int = Query(1980, ge=1900), year_max: int = Query(2025, ge=1900)):
    results, facets = fts_search(q, domain, compliance, replication, year_min, year_max)
    return {"results": results, "facets": facets}

@app.get("/")
def root():
    return {"message": "Truth Scaffold API is running. Try /search or /docs"}


@app.get("/healthz")
def healthz():
    return {"ok": True}

