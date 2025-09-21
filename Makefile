.PHONY: venv install sanity migrate ingest

PY=python3
VENV=.venv

venv:
	$(PY) -m venv $(VENV)

install: venv
	. $(VENV)/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

sanity:
	. $(VENV)/bin/activate && $(PY) sanity_check.py

migrate:
	. $(VENV)/bin/activate && $(PY) scripts/migrate.py

# Use CSV_PATH to override file name/location
ingest:
	. $(VENV)/bin/activate && $(PY) scripts/ingest_csv.py
run:
	. $(VENV)/bin/activate && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
