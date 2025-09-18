# truth-scaffold
# Truth Scaffold API

Backend service for searching and explaining mental health research with transparency-first filters.

pip install -r requirements.txt -c constraints.txt


## Features
- Postgres full-text search (FTS) + optional pgvector embeddings
- Compliance / replication / demographics flags
- REST API (FastAPI)
- Deployable on Render (Docker + render.yaml)

## Quick start (local)

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env          # then edit .env
psql $PG_DSN -f sql/001_init.sql
python scripts/ingest_csv.py --csv data/records.csv
uvicorn app.api:app --reload
