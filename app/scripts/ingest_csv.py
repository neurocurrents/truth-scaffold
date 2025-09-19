import argparse, os, math
import pandas as pd
from sqlalchemy import create_engine, text

PG_DSN = os.getenv("PG_DSN")
engine = create_engine(PG_DSN)

BOOL_TRUE = {"1","true","t","y","yes","True","TRUE"}
def to_bool(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if s == "": return None
    return s in BOOL_TRUE

def first_truthy(*vals):
    for v in vals:
        if pd.notna(v) and str(v).strip() != "":
            return v
    return None

def pick_domain(row):
    # pick the first True flag → domain label
    if to_bool(row.get("is_nfb")):  return "neurofeedback"
    if to_bool(row.get("is_tms")):  return "tms"
    if to_bool(row.get("is_psych")): return "psychotherapy"
    if to_bool(row.get("is_pharma")): return "pharmacotherapy"
    # fallback: look at feature/type keywords
    feat = str(first_truthy(row.get("feature"), row.get("type")) or "").lower()
    if "neurofeedback" in feat: return "neurofeedback"
    if "tms" in feat: return "tms"
    if "cbt" in feat or "therapy" in feat or "psych" in feat: return "psychotherapy"
    if "ssri" in feat or "snri" in feat or "drug" in feat or "med" in feat: return "pharmacotherapy"
    return None

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["pmid"]   = df.get("pmid")
    out["doi"]    = df.get("doi")
    out["title"]  = df.get("title")
    out["abstract"] = df.get("abstract")

    # journal: prefer journal_final then journal
    out["journal"] = df.get("journal_final").where(df.get("journal_final").notna(), df.get("journal"))

    # year: prefer pub_year_final, else year
    year = pd.to_numeric(df.get("pub_year_final").fillna(df.get("year")), errors="coerce")
    out["year"] = year

    # decade
    out["decade"] = (year // 10 * 10).astype("Int64")

    # transparency flags
    # has_demo: prefer explicit boolean/binary, else text presence
    has_demo = df.get("has_demo")
    if has_demo is None:
        has_demo = df.get("has_demo_bin")
    out["has_demo"] = has_demo.apply(to_bool) if has_demo is not None else df.get("has_demo_text").notna()

    has_outcome = df.get("has_outcome")
    if has_outcome is None:
        has_outcome = df.get("has_outcome_col")
    out["has_outcome"] = has_outcome.apply(to_bool) if has_outcome is not None else df.get("has_outcome_text").notna()

    # compliance
    # take provided category if present; else map bin/boolean
    cat = df.get("compliance_category")
    if cat is None or cat.isna().all():
        binv = df.get("compliance_category_bin")
        if binv is not None:
            cat = binv.map({1: "compliant", "1":"compliant", "compliant":"compliant",
                            0: "noncompliant", "0":"noncompliant", "noncompliant":"noncompliant"})
        else:
            cat = df.get("is_compliant").apply(lambda v: "compliant" if to_bool(v) else "noncompliant")
    out["compliance_category"] = cat

    # domain from flags/keywords
    out["domain"] = df.apply(pick_domain, axis=1)

    # replication placeholders (fill later if you have cols)
    out["is_replication"] = None
    out["replication_outcome"] = None

    # effect_size, oa fields if you have them; else null
    out["effect_size"] = None
    # is_paywalled: map to oa_status
    oa_status = df.get("is_paywalled").apply(lambda v: "closed" if to_bool(v) else "open") if "is_paywalled" in df.columns else None
    out["oa_status"] = oa_status
    out["oa_url"] = None

    # drop rows with no pmid and no doi (need at least one id)
    out = out[~(out["pmid"].isna() & out["doi"].isna())].copy()

    # final column order to match SQL schema
    cols = ["pmid","doi","title","abstract","journal","year","decade","domain",
            "medication_class","is_replication","replication_outcome","has_demo","has_outcome",
            "compliance_category","effect_size","oa_status","oa_url"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]

def upsert_postgres(df_norm: pd.DataFrame):
    with engine.begin() as conn:
        df_norm.to_sql("records", conn, if_exists="append", index=False, method="multi", chunksize=1000)
        conn.execute(text(
            "UPDATE records SET fts = to_tsvector('english', coalesce(title,'') || ' ' || coalesce(abstract,''));"
        ))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    raw = pd.read_csv(args.csv, sep=None, engine="python")  # auto-detect comma/tab
    df_norm = normalize(raw)
    upsert_postgres(df_norm)
    print(f"Ingested {len(df_norm)} normalized records → Postgres (FTS ready)")

if __name__ == "__main__":
    if not PG_DSN:
        raise SystemExit("PG_DSN is not set. Export it or put it in .env")
    main()

