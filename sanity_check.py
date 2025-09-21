import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()  # loads .env if present

dsn = os.getenv("PG_DSN")
if not dsn:
    raise SystemExit("PG_DSN not set. Put it in .env (see .env.example).")

engine = create_engine(dsn, pool_pre_ping=True, future=True)
print("Connecting to DB...")
with engine.begin() as conn:
    version = conn.execute(text("SELECT version();")).scalar_one()
    print("OK. Server version:", version)
    # show existing tables
    tables = conn.execute(text("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' ORDER BY 1;
    """)).fetchall()
    print("Public tables:", [t[0] for t in tables])
print("All good ✅")
