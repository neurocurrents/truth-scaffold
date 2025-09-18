from typing import Tuple, List, Dict, Any
from sqlalchemy import text
from .db import engine

def fts_search(q: str, domain: str | None, comp: str | None, rep: str | None,
               year_min: int, year_max: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    sql = text("""
        WITH base AS (
          SELECT *,
                 CASE WHEN :q = '' THEN 0.0 ELSE ts_rank_cd(fts, plainto_tsquery('english', :q)) END AS rank
          FROM records
          WHERE year BETWEEN :ymin AND :ymax
            AND (:domain IS NULL OR :domain = 'all' OR domain = :domain)
            AND (:comp   IS NULL OR :comp   = 'all' OR compliance_category = :comp)
            AND (:rep    IS NULL OR :rep    = 'all' OR replication_outcome = :rep)
            AND (:q = '' OR fts @@ plainto_tsquery('english', :q))
        )
        SELECT * FROM base
        ORDER BY rank DESC NULLS LAST, year DESC
        LIMIT 20
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {
            "q": q, "ymin": year_min, "ymax": year_max,
            "domain": domain, "comp": comp, "rep": rep
        }).mappings().all()

        facets: Dict[str, Any] = {}
        res1 = conn.execute(text("SELECT domain, count(*) c FROM records GROUP BY domain ORDER BY c DESC LIMIT 25")).all()
        facets["domain"] = [{"key": r[0] or "unknown", "count": r[1]} for r in res1]
        res2 = conn.execute(text("SELECT compliance_category, count(*) c FROM records GROUP BY compliance_category ORDER BY c DESC LIMIT 25")).all()
        facets["compliance_category"] = [{"key": r[0] or "unknown", "count": r[1]} for r in res2]
        res3 = conn.execute(text("SELECT COALESCE(replication_outcome,'not reported') k, count(*) c FROM records GROUP BY 1 ORDER BY c DESC LIMIT 25")).all()
        facets["replication_outcome"] = [{"key": r[0], "count": r[1]} for r in res3]

    return [dict(r) for r in rows], facets
