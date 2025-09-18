import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    pg_dsn: str = os.getenv("PG_DSN", "postgresql+psycopg2://user:pass@localhost:5432/truth_scaffold")

settings = Settings()
