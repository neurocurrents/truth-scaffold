import argparse, os
import pandas as pd
from sqlalchemy import create_engine, text

PG_DSN = os.getenv("PG_DSN")
if not PG_DSN:
    raise SystemExit("PG_DSN is not set. Put it in .env or export it.")

engine = create_engine(PG_DSN, pool_pre_ping=True, future=True)

BOOL_TRUE = {"1","true","t","y","yes","True","TRUE"}

def to_bool(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if s == "": return None
    return s in BOOL_TRUE

def first_nonempty(*vals):
    for v in vals:
        if v is None: 
            continue
        s = str(v).strip()
        if s != "" and s.lower() != "nan":
            return v
    return None

def derive_year(df: pd.DataFrame) -> pd.Series:
    # canonical 'year' from pub_year_final -> year_analytic -> year
    y = pd.to_numeric(df.get("pub_year_final"), errors="coerce")
    if y is None or y.isna().all():
        y = pd.to_numeric(df.get("year_analytic"), errors="coerce")
    if y is None or y.isna().all():
        y = pd.to_numeric(df.get("year"), errors="coerce")
    return y

def derive_decade(year: pd.Series) -> pd.Series:
    return (year // 10 * 10).astype("Int64")

def derive_has_demo(df: pd.DataFrame) -> pd.Series:
    # prefer explicit bool/bin, else text presence
    col = df.get("has_demo")
    if col is None: col = df.get("has_demo_bin")
    if col is not None:
        return col.apply(to_bool)
    txt = df.get("has_demo_text")
    return txt.notna() if txt is not None else None

def derive_has_outcome(df: pd.DataFrame) -> pd.Series:
    col = df.get("has_outcome")
    if col is None: col = df.get("has_outcome_col")
    if col is not None:
        return col.apply(to_bool)
    txt = df.get("has_outcome_text")
    return txt.notna() if txt is not None else None

def derive_compliance(df: pd.DataFrame) -> pd.Series:
    # keep canonical name: compliance_category
    cat = df.get("compliance_category")
    if cat is not None and not cat.isna().all():
        return cat
    binv = df.get("compliance_category_bin")
    if binv is not None:
        return binv.map({
            1: "compliant", "1": "compliant", "compliant": "compliant",
            0: "noncompliant", "0": "noncompliant", "noncompliant": "noncompliant"
        })
    # fallback from boolean
    compl = df.get("is_compliant")
    if compl is not None:
        return compl.apply(lambda v: "compliant" if to_bool(v) else "noncompliant")
    return None

def derive_domain(row: pd.Series) -> str | None:
    # priority from flags (adjust as you like)
    if to_bool(row.get("is_nfb")):   return "neurofeedback"
    if to_bool(row.get("is_tms")):   return "tms"
    if to_bool(row.get("is_psych")): return "psychotherapy"
    if to_bool(row.get("is_pharma")):return "pharmacotherapy"
    # explicit domain column?
    dom = first_nonempty(row.get("domain"), row.get("domain_m99"))
    if dom: return str(dom).lower()
    # keyword fallback
    feat = str(first_nonempty(row.get("feature"), row.get("type")) or "").lower()
    if "neurofeedback" in feat: return "neurofeedback"
    if "tms" in feat:           return "tms"
    if "cbt" in feat or "therapy" in feat or "psych" in feat: return "psychotherapy"
    if "ssri" in feat or "snri" in feat or "drug" in feat or "med" in feat: return "pharmacotherapy"
    return None

def derive_is_replication(df: pd.DataFrame) -> pd.Series:
    for name in ("is_replication", "is_replication_m99", "replication_flag"):
        if name in df.columns:
            return df[name].apply(to_bool)
    return None

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()

    # core ids/text
    out["pmid"]     = df.get("pmid")
    out["doi"]      = df.get("doi")
    out["title"]    = df.get("title")
    out["abstract"] = df.get("abstract")

    # journal prefer 'journal_final' then 'journal'
    out["journal"]  = df.get("journal_final").where(df.get("journal_final").notna(), df.get("journal"))

    # year + decade
    year = derive_year(df)
    out["year"]   = year
    out["decade"] = derive_decade(year)

    # domain / medication_class
    out["domain"]            = df.apply(derive_domain, axis=1)
    out["medication_class"]  = df.get("medication_class")

    # replication
    out["is_replication"]      = derive_is_replication(df)
    out["replication_outcome"] = df.get("replication_outcome")

    # transparency flags
    out["has_demo"]    = derive_has_demo(df)
    out["has_outcome"] = derive_has_outcome(df)

    # compliance
    out["compliance_category"] = derive_compliance(df)

    # effect size + OA
    out["effect_size"] = pd.to_numeric(df.get("effect_size"), errors="coerce") if "effect_size" in df.columns else None
    # is_paywalled -> oa_status
    if "is_paywalled" in df.columns:
        out["oa_status"] = df["is_paywalled"].apply(lambda v: "closed" if to_bool(v) else "open")
    else:
        out["oa_status"] = None
    out["oa_url"] = df.get("oa_url")

    # drop rows missing both pmid and doi
    out = out[~(out["pmid"].isna() & out["doi"].isna())].copy()

    # canonical order to match your records table
    cols = [
        "pmid","doi","title","abstract","journal","year","decade","domain",
        "medication_class","is_replication","replication_outcome",
        "has_demo","has_outcome","compliance_category",
        "effect_size","oa_status","oa_url"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]

def upsert_postgres(df_norm: pd.DataFrame):
    with engine.begin() as conn:
        df_norm.to_sql("records", conn, if_exists="append", index=False, method="multi", chunksize=1000)
        # Build FTS column after load
        conn.execute(text(
            "UPDATE records SET fts = to_tsvector('english', coalesce(title,'') || ' ' || coalesce(abstract,''));"
        ))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--truncate", action="store_true", help="Clear records table before ingest")
    args = ap.parse_args()

    if args.truncate:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE records;"))

    # auto-detect delimiter
    raw = pd.read_csv(args.csv, sep=None, engine="python")
    df_norm = normalize(raw)
    upsert_postgres(df_norm)
    print(f"Ingested {len(df_norm)} rows → Postgres (FTS ready)")

if __name__ == "__main__":
    main()


