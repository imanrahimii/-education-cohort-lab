import csv
import sqlite3
import unittest
from pathlib import Path

from queries import METRICS, REJECTED, STUDENTS, SURVEY
from statistics_core import standardised_gap

ROOT = Path(__file__).resolve().parents[1]


def load(batch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
      CREATE TABLE bronze_students(student_id TEXT, cohort TEXT, age_band TEXT,
                                   field TEXT, continued TEXT, revision TEXT);
      CREATE TABLE bronze_survey(student_id TEXT, outcome TEXT, result TEXT);
    """)
    sources = [("students_batch_1.csv", "bronze_students", 6), ("survey.csv", "bronze_survey", 3)]
    if batch == 2:
        sources.append(("students_batch_2.csv", "bronze_students", 6))
    for filename, table, width in sources:
        with (ROOT / "data" / filename).open(newline="") as file:
            reader = csv.reader(file)
            next(reader)
            params = ", ".join("?" for _ in range(width))
            conn.executemany(f"INSERT INTO {table} VALUES ({params})", reader)
    names = {name: name for name in ("bronze_students", "bronze_survey", "silver_students", "silver_survey")}
    for name, query in [("rejected", REJECTED), ("silver_students", STUDENTS),
                        ("silver_survey", SURVEY), ("gold", METRICS)]:
        conn.execute(f"CREATE VIEW {name} AS {query.format(**names)}")
    return conn


class ProjectTests(unittest.TestCase):
    def test_late_outcomes(self):
        for batch, expected in [(1, 738), (2, 769)]:
            with load(batch) as db:
                row = db.execute("SELECT COUNT(*), COUNT(continued) FROM silver_students").fetchone()
                self.assertEqual(tuple(row), (800, expected))

    def test_quarantine_and_duplicate_delivery(self):
        with load(2) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM rejected").fetchone()[0], 1)
            before = [tuple(row) for row in db.execute("SELECT * FROM gold ORDER BY 1, 2, 3")]
            db.execute("INSERT INTO bronze_students SELECT * FROM bronze_students")
            after = [tuple(row) for row in db.execute("SELECT * FROM gold ORDER BY 1, 2, 3")]
            self.assertEqual(before, after)

    def test_survey_join_keeps_eligible_population(self):
        with load(2) as db:
            rows = db.execute("SELECT outcome, SUM(eligible), SUM(observed) FROM gold GROUP BY outcome")
            for _, eligible, observed in rows:
                self.assertEqual(eligible, 800)
                self.assertTrue(0 < observed < eligible)

    def test_adjustment(self):
        with load(2) as db:
            for outcome in ("continuation", "experience", "employment"):
                result = standardised_gap(list(db.execute("SELECT * FROM gold WHERE outcome = ?", (outcome,))))
                low, high = result["ci95_pp"]
                self.assertLess(low, result["adjusted_gap_pp"])
                self.assertGreater(high, result["adjusted_gap_pp"])
                self.assertTrue(0 <= result["p_value_unadjusted"] <= 1)

    def test_notebook_python_syntax(self):
        for notebook in (ROOT / "notebooks").glob("*.py"):
            compile(notebook.read_text(), str(notebook), "exec")
