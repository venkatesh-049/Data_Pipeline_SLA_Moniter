import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import inspect, text

from sla_monitor.config import PROJECT_ROOT, get_database_path
from sla_monitor.database import create_database_engine, initialize_database


class DatabaseTests(unittest.TestCase):
    def test_database_path_uses_sqlite_setting_from_environment(self):
        with patch("sla_monitor.config._FILE_SETTINGS", {}):
            with patch.dict(
                os.environ, {"SQLITE_DB_PATH": "data/test.db"}, clear=True
            ):
                self.assertEqual(
                    get_database_path(), PROJECT_ROOT / "data" / "test.db"
                )

    def test_database_path_defaults_to_project_sqlite_database(self):
        with patch("sla_monitor.config._FILE_SETTINGS", {}):
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    get_database_path(), PROJECT_ROOT / "database" / "pipeline.db"
                )

    def test_database_engine_rejects_non_sqlite_urls(self):
        with self.assertRaisesRegex(ValueError, "only SQLite"):
            create_database_engine("postgresql://user:password@localhost/db")

    def test_sqlite_schema_initializes_all_monitoring_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_database_engine(Path(directory) / "monitor.db")
            try:
                initialize_database(engine)
                self.assertEqual(
                    set(inspect(engine).get_table_names()),
                    {"runs", "dataset_results", "rejects", "alerts"},
                )
                with engine.connect() as connection:
                    foreign_keys = connection.execute(
                        text("PRAGMA foreign_keys")
                    ).scalar_one()
                self.assertEqual(foreign_keys, 1)
            finally:
                engine.dispose()

    def test_schema_sql_executes_on_sqlite(self):
        schema_path = PROJECT_ROOT / "database" / "schema.sql"
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "schema.db"
            engine = create_database_engine(database_path)
            connection = sqlite3.connect(database_path)
            try:
                connection.executescript(schema_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    {
                        table
                        for table in inspect(engine).get_table_names()
                        if not table.startswith("sqlite_")
                    },
                    {"runs", "dataset_results", "rejects", "alerts"},
                )
            finally:
                connection.close()
                engine.dispose()


if __name__ == "__main__":
    unittest.main()
