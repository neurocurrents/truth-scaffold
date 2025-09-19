FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential libpq-dev curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt constraints.txt ./
RUN pip install --no-cache-dir -r requirements.txt -c constraints.txt


COPY app ./app
COPY sql ./sql
COPY scripts ./scripts

EXPOSE 10000

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "10000"]


