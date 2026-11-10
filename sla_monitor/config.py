from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
_FILE_SETTINGS = dotenv_values(ENV_FILE)


def get_database_path() -> Path:
    """Return the SQLite file path from SQLITE_DB_PATH or the local default."""
    process_value = os.getenv("SQLITE_DB_PATH", "").strip()
    file_value = (_FILE_SETTINGS.get("SQLITE_DB_PATH") or "").strip()
    configured_path = process_value or file_value or "database/pipeline.db"
    path = Path(configured_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path
