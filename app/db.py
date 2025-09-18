from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .config import settings

engine: Engine = create_engine(settings.pg_dsn, pool_pre_ping=True, future=True)

def fetch_paper(pmid: str) -> dict | None:
    sql = text("""
        SELECT pmid, doi, title, abstract, journal, year, decade, domain, medication_class,
               is_replication, replication_outcome, has_demo, has_outcome,
               compliance_category, effect_size, oa_status, oa_url
        FROM records WHERE pmid=:pmid
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"pmid": pmid}).mappings().first()
        return dict(row) if row else None
