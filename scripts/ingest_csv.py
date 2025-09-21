# scripts/ingest_csv.py
import os, io, sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

BOOL_TRUE  = {"1","true","t","y","yes","on","True","TRUE"}
BOOL_FALSE = {"0","false","f","n","no","off","False","FALSE"}

def to_bool(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if s == "": return None
    if s in BOOL_TRUE:  return True
    if s in BOOL_FALSE: return False
    try:
        return bool(int(float(s)))
    except Exception:
        return None

def infer_study_type(title, abstract):
    s = f"{title or ''} {abstract or ''}".lower()
    if "meta-analysis" in s or "systematic review" in s: return "meta-analysis"
    if "randomized" in s or "randomised" in s or " rct " in s: return "rct"
    if "replication" in s or "reproduc" in s: return "replication"
    if "case report" in s: return "case-report"
    if "cross-sectional" in s: return "observational"
    if "cohort" in s or "prospective" in s or "retrospective" in s: return "observational"
    if "pilot" in s or "feasibility" in s: return "pilot"
    if "review" in s: return "review"
    return "other"

def infer_outcome_sign(text):
    s = (text or "").lower()
    pos = any(k in s for k in [
        "significant improvement", "improved", "improvement", "reduction in symptoms",
        "reduced symptoms", "benefit", "effective", "efficacious"
    ])
    neg = any(k in s for k in [
        "no significant difference", "no difference", "ns difference", "worsened",
        "worsening", "increased symptoms", "adverse", "ineffective", "not effective"
    ])
    if pos and not neg: return "positive"
    if neg and not pos: return "negative"
    if "no significant" in s or "no difference" in s or "ns " in s: return "neutral"
    return "unclear"

def ensure_min_columns(engine):
    """Add columns we need if they don't exist. Safe/idempotent."""
    ddl = """
    ALTER TABLE records
      ADD COLUMN IF NOT EXISTS study_type       text,
      ADD COLUMN IF NOT EXISTS is_compliant     boolean,
      ADD COLUMN IF NOT EXISTS outcome_present  boolean,
      ADD COLUMN IF NOT EXISTS outcome_positive boolean,
      ADD COLUMN IF NOT EXISTS outcome_sign     text;
    """
    with engine.begin() as c:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                c.execute(text(s))

def main():
    load_dotenv()
    dsn = os.getenv("PG_DSN")
    if not dsn:
        sys.exit("PG_DSN is not set. Put it in .env")

    csv_path = os.getenv("CSV_PATH", "records.csv")
    if not os.path.exists(csv_path):
        sys.exit(f"Missing {csv_path}. Place your cleaned CSV there or set CSV_PATH.")

    print(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)  # keep this line active

    # ----- Derive study_type & is_compliant if missing in CSV -----
    if "study_type" not in df.columns:
        df["study_type"] = [infer_study_type(t, a) for t, a in zip(df.get("title"), df.get("abstract"))]

    if "is_compliant" not in df.columns:
        if {"demo_present","outcome_present"}.issubset(df.columns):
            demo = pd.to_numeric(df["demo_present"], errors="coerce").fillna(0)
            outc = pd.to_numeric(df["outcome_present"], errors="coerce").fillna(0)
            df["is_compliant"] = (demo > 0) & (outc > 0)
        else:
            combined = (df.get("title").fillna("") + " " + df.get("abstract").fillna("")).str.lower()
            demo_kw = combined.str.contains(r"\b(sex|gender|male|female|race|ethnic|ethnicity)\b", regex=True)
            out_kw  = combined.str.contains(r"\b(outcome|effect|response|score|change|improvement|adverse)\b", regex=True)
            df["is_compliant"] = demo_kw & out_kw

    # ----- Outcome features -----
    # 1) Map is_1 -> outcome_positive if present
    if "is_1" in df.columns and "outcome_positive" not in df.columns:
        df["outcome_positive"] = (pd.to_numeric(df["is_1"], errors="coerce").fillna(0) > 0)

    # 2) Ensure/derive outcome_present if missing
    if "outcome_present" in df.columns:
        df["outcome_present"] = pd.to_numeric(df["outcome_present"], errors="coerce").fillna(0) > 0
    else:
        combined = (df.get("title").fillna("") + " " + df.get("abstract").fillna("")).str.lower()
        df["outcome_present"] = combined.str.contains(
            r"\b(outcome|effect|response|score|change|difference|improv|worsen|adverse)\b", regex=True
        )

    # 3) Derive outcome_sign if missing
    if "outcome_sign" not in df.columns:
        combo = (df.get("title").fillna("") + " " + df.get("abstract").fillna(""))
        df["outcome_sign"] = combo.map(infer_outcome_sign)

    # If outcome_positive still missing, set from outcome_sign
    if "outcome_positive" not in df.columns and "outcome_sign" in df.columns:
        df["outcome_positive"] = df["outcome_sign"].map({"positive": True, "negative": False}).astype("boolean")

    # ----- Clean pmid & drop invalid -----
    if "pmid" not in df.columns:
        sys.exit("CSV is missing 'pmid' column.")
    df["pmid"] = pd.to_numeric(df["pmid"], errors="coerce")
    before = len(df)
    df = df[df["pmid"].notna()].copy()
    dropped = before - len(df)
    df["pmid"] = df["pmid"].astype("Int64")
    print(f"PMID cleaned: kept {len(df)}, dropped {dropped} rows with missing/invalid pmid")

    engine = create_engine(dsn, pool_pre_ping=True, future=True)

    # Ensure the DB has the columns we want to load (safe, idempotent)
    ensure_min_columns(engine)

    # Introspect current DB schema for 'records'
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='records'
            ORDER BY ordinal_position
        """)).fetchall()
    if not rows:
        sys.exit("Table 'records' not found. Run `make migrate` first.")

    db_cols  = [r[0] for r in rows]
    db_types = {r[0]: r[1].lower() for r in rows}

    # Column overlap (preserve DB order). Must include pmid.
    cols = [c for c in db_cols if c in df.columns]
    if "pmid" not in cols:
        sys.exit("'pmid' must exist in CSV and DB.")
    if not cols:
        sys.exit("No overlapping columns between CSV and 'records' table.")

    # Coerce DataFrame dtypes to match DB types
    for c in cols:
        t = db_types.get(c, "text")
        if t in ("smallint","integer","bigint"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        elif t in ("double precision","real","numeric","decimal"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        elif t == "boolean":
            df[c] = df[c].map(to_bool) if df[c].dtype != bool else df[c]
        else:
            df[c] = df[c].astype("string")

    # Recreate staging table with exact columns from 'records'
    with engine.begin() as conn:
        sel_cols = ", ".join(f'"{c}"' for c in cols)
        conn.execute(text("DROP TABLE IF EXISTS records_stage"))
        conn.execute(text(f'CREATE UNLOGGED TABLE records_stage AS SELECT {sel_cols} FROM records WHERE false'))
        conn.execute(text("TRUNCATE TABLE records_stage"))

    # COPY into staging (treat empty strings as NULL)
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False)
    buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            col_list = ", ".join(f'"{c}"' for c in cols)
            cur.copy_expert(f"COPY records_stage ({col_list}) FROM STDIN WITH CSV HEADER NULL ''", buf)
        raw.commit()
    finally:
        raw.close()

    # Upsert into target
    with engine.begin() as conn:
        insert_cols = ", ".join(f'"{c}"' for c in cols)
        select_cols = ", ".join(f'"{c}"' for c in cols)
        update_pairs = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != "pmid")
        upsert_sql = (
            f'INSERT INTO records ({insert_cols}) '
            f'SELECT {select_cols} FROM records_stage '
            f'ON CONFLICT (pmid) DO UPDATE SET {update_pairs};'
        )
        conn.execute(text(upsert_sql))

    print(f"Ingest complete ✅  Rows staged: {len(df)}  Columns used: {len(cols)} -> {cols}")

if __name__ == "__main__":
    main()
