.PHONY: venv install sanity migrate ingest

PY=python3
VENV=.venv

venv:
	$(PY) -m venv $(VENV)

install: venv
	. $(VENV)/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

sanity:
	. $(VENV)/bin/activate && $(PY) sanity.py

migrate:
	. $(VENV)/bin/activate && $(PY) scripts/migrate.py

# Use CSV_PATH to override file name/location
ingest:
	. $(VENV)/bin/activate && $(PY) scripts/ingest_csv.py
