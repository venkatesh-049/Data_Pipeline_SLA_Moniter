import csv
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from sla_monitor.generator import SAMPLE_AS_OF, generate_sample
from sla_monitor.pipeline import PipelineError, run_pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.datasets = self.root / "datasets.csv"
        self.loads = self.root / "loads.csv"
        self.db = self.root / "monitor.db"
        self.as_of = datetime(2025, 1, 1, 3, 0, tzinfo=timezone.utc)
        self.write_datasets(
            [
                ["sales", "Daily sales", "2025-01-01T02:00:00Z", "30", "30"],
                ["inventory", "Hourly inventory", "2025-01-01T02:00:00Z", "30", "60"],
            ]
        )

    def tearDown(self):
        self.temporary.cleanup()

    def write_datasets(self, rows):
        with self.datasets.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(
                [
                    "dataset_id",
                    "dataset_name",
                    "expected_arrival_at",
                    "completion_sla_minutes",
                    "freshness_sla_minutes",
                ]
            )
            writer.writerows(rows)

    def write_loads(self, rows, header=None):
        with self.loads.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(
                header
                or [
                    "dataset_id",
                    "load_id",
                    "arrived_at",
                    "completed_at",
                    "row_count",
                    "source_file",
                ]
            )
            writer.writerows(rows)

    def run_batch(self):
        return run_pipeline(self.datasets, self.loads, self.db, self.as_of)

    def test_sample_has_expected_alerts_and_traceable_rejects(self):
        sample_dir = self.root / "sample"
        datasets, loads = generate_sample(sample_dir)
        report = run_pipeline(datasets, loads, self.db, SAMPLE_AS_OF)
        by_id = {row["dataset_id"]: row for row in report["results"]}
        self.assertEqual(by_id["sales_daily"]["arrival_status"], "on_time")
        self.assertEqual(by_id["inventory_hourly"]["arrival_status"], "late")
        self.assertEqual(by_id["inventory_hourly"]["completion_status"], "breached")
        self.assertEqual(by_id["customers_daily"]["arrival_status"], "missing")
        self.assertEqual(by_id["events_hourly"]["arrival_status"], "invalid")
        self.assertEqual(report["run"]["rejected_count"], 4)
        self.assertTrue(all(item["source_line"] >= 2 for item in report["rejects"]))
        self.assertIn(
            "ISO-8601",
            next(item["reason"] for item in report["rejects"] if item["source_line"] == 8),
        )
        self.assertIn("missing_load", {item["alert_code"] for item in report["alerts"]})

    def test_empty_load_batch_marks_each_expected_dataset_missing(self):
        self.write_loads([])
        report = self.run_batch()
        self.assertEqual(report["run"]["loaded_count"], 0)
        self.assertEqual(report["run"]["expected_count"], 2)
        self.assertTrue(
            all(row["arrival_status"] == "missing" for row in report["results"])
        )

    def test_empty_expected_schedule_is_a_valid_empty_batch(self):
        self.write_datasets([])
        self.write_loads([])
        report = self.run_batch()
        self.assertEqual(report["run"]["output_count"], 0)
        self.assertEqual(report["run"]["rejected_count"], 0)
        self.assertEqual(report["run"]["failure_reason"], "no failure")

    def test_missing_required_value_is_quarantined(self):
        self.write_loads(
            [["sales", "load-1", "2025-01-01T02:00:00Z", "", "1", "sales.csv"]]
        )
        report = self.run_batch()
        self.assertEqual(report["rejects"][0]["reason_code"], "invalid_load")
        sales = next(row for row in report["results"] if row["dataset_id"] == "sales")
        self.assertEqual(sales["arrival_status"], "invalid")
        self.assertEqual(report["run"]["loaded_count"], 0)

    def test_future_schedule_is_not_alerted_as_missing(self):
        self.write_datasets(
            [["sales", "Daily sales", "2025-01-01T04:00:00Z", "30", "30"]]
        )
        self.write_loads([])
        report = self.run_batch()
        self.assertEqual(report["results"][0]["arrival_status"], "not_due")
        self.assertNotIn("missing_load", {item["alert_code"] for item in report["alerts"]})

    def test_duplicate_key_and_second_load_are_rejected(self):
        first = ["sales", "load-1", "2025-01-01T02:00:00Z", "2025-01-01T02:10:00Z", "1", "a.csv"]
        second = ["sales", "load-1", "2025-01-01T02:00:00Z", "2025-01-01T02:11:00Z", "1", "b.csv"]
        third = ["inventory", "load-2", "2025-01-01T02:00:00Z", "2025-01-01T02:10:00Z", "1", "c.csv"]
        fourth = ["inventory", "load-3", "2025-01-01T02:00:00Z", "2025-01-01T02:10:00Z", "1", "d.csv"]
        self.write_loads([first, second, third, fourth])
        report = self.run_batch()
        codes = [item["reason_code"] for item in report["rejects"]]
        self.assertEqual(codes, ["duplicate_load", "multiple_loads_for_schedule"])
        self.assertEqual(report["run"]["loaded_count"], 2)

    def test_rejected_event_does_not_claim_a_later_valid_event_key(self):
        self.write_loads(
            [
                ["sales", "load-1", "bad-time", "2025-01-01T02:10:00Z", "1", "bad.csv"],
                ["sales", "load-1", "2025-01-01T02:00:00Z", "2025-01-01T02:10:00Z", "1", "good.csv"],
            ]
        )
        report = self.run_batch()
        self.assertEqual(report["run"]["loaded_count"], 1)
        self.assertEqual(report["rejects"][0]["reason_code"], "invalid_load")
        sales = next(row for row in report["results"] if row["dataset_id"] == "sales")
        self.assertEqual(sales["arrival_status"], "on_time")

    def test_completion_before_arrival_is_quarantined(self):
        self.write_loads(
            [["sales", "load-1", "2025-01-01T02:10:00Z", "2025-01-01T02:05:00Z", "1", "sales.csv"]]
        )
        report = self.run_batch()
        self.assertEqual(report["rejects"][0]["reason_code"], "completion_before_arrival")

    def test_future_completion_and_unknown_reference_are_rejected(self):
        self.write_loads(
            [
                ["sales", "future", "2025-01-01T02:10:00Z", "2025-01-01T03:01:00Z", "1", "future.csv"],
                ["ghost", "unknown", "2025-01-01T02:10:00Z", "2025-01-01T02:20:00Z", "1", "ghost.csv"],
            ]
        )
        report = self.run_batch()
        self.assertEqual(
            [item["reason_code"] for item in report["rejects"]],
            ["completion_in_future", "unknown_dataset"],
        )

    def test_exact_arrival_completion_and_freshness_boundaries_pass(self):
        self.write_loads(
            [["sales", "load-1", "2025-01-01T02:00:00Z", "2025-01-01T02:30:00Z", "0", "sales.csv"]]
        )
        report = self.run_batch()
        sales = next(row for row in report["results"] if row["dataset_id"] == "sales")
        self.assertEqual(sales["arrival_status"], "on_time")
        self.assertEqual(sales["completion_status"], "within_sla")
        self.assertEqual(sales["freshness_status"], "fresh")
        self.assertEqual(sales["freshness_minutes"], 30.0)
        self.assertEqual(sales["row_count"], 0)

    def test_schema_change_fails_run_and_records_failure_reason(self):
        self.write_loads([], header=["dataset_id", "load_id", "unexpected"])
        with self.assertRaisesRegex(PipelineError, "schema mismatch"):
            self.run_batch()
        connection = sqlite3.connect(self.db)
        try:
            row = connection.execute(
                "SELECT status, failure_reason FROM runs"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[0], "failed")
        self.assertIn("unexpected columns", row[1])

    def test_missing_input_is_recorded_as_a_failed_run(self):
        self.write_loads([])
        self.loads.unlink()
        with self.assertRaisesRegex(PipelineError, "cannot read"):
            self.run_batch()
        connection = sqlite3.connect(self.db)
        try:
            status, reason = connection.execute(
                "SELECT status, failure_reason FROM runs"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(status, "failed")
        self.assertIn("cannot read", reason)

    def test_identical_successful_rerun_is_idempotent(self):
        self.write_loads(
            [["sales", "load-1", "2025-01-01T02:00:00Z", "2025-01-01T02:10:00Z", "10", "sales.csv"]]
        )
        first = self.run_batch()
        second = self.run_batch()
        self.assertEqual(first["run"]["run_id"], second["run"]["run_id"])
        self.assertEqual(first["run"]["failure_reason"], "no failure")
        connection = sqlite3.connect(self.db)
        try:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 1
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM dataset_results").fetchone()[0],
                2,
            )
        finally:
            connection.close()

    def test_malformed_record_is_rejected_without_losing_valid_load(self):
        self.loads.write_text(
            "dataset_id,load_id,arrived_at,completed_at,row_count,source_file\n"
            "sales,good,2025-01-01T02:00:00Z,2025-01-01T02:10:00Z,10,sales.csv\n"
            "inventory,bad,not-a-time,2025-01-01T02:10:00Z,10,bad.csv\n",
            encoding="utf-8",
        )
        report = self.run_batch()
        self.assertEqual(report["run"]["loaded_count"], 1)
        self.assertEqual(report["run"]["rejected_count"], 1)
        self.assertIn("ISO-8601", report["rejects"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
