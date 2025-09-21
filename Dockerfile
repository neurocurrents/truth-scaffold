# ---- base ----
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8000

WORKDIR /app

# (optional) build tools; usually not needed with psycopg2-binary wheels,
# but harmless if a lib needs compiling
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install deps first (better cache)
COPY requirements.txt .
RUN python -m pip install --upgrade pip && pip install -r requirements.txt

# Copy app code
COPY app ./app
COPY scripts ./scripts
COPY sanity_check.py ./sanity_check.py

# (Render ignores EXPOSE; still fine locally)
EXPOSE 8000

# Production server (no --reload)
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
