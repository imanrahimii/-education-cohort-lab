"""Regenerate the Databricks source notebooks and deterministic synthetic inputs."""
import csv
import random
from pathlib import Path

ROOT = Path(__file__).parent


def write_csv(name, header, rows):
    with (ROOT / "data" / name).open("w", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(header.split(","))
        writer.writerows(rows)


rng = random.Random(203)
students, surveys, updates = [], [], []
for i in range(800):
    sid = f"S{i:04}"
    cohort = "Domestic" if i < 400 else "International"
    domestic = cohort == "Domestic"
    age = "25+" if rng.random() < (0.45 if domestic else 0.23) else "Under 25"
    field = rng.choice(["Business", "Engineering", "Education"])
    continued = int(rng.random() < (0.70 + 0.07 * domestic + 0.08 * (age == "Under 25")))
    students.append((sid, cohort, age, field, "" if i % 13 == 0 else continued, 1))
    if i % 13 == 0 and i % 2 == 0:
        updates.append((sid, cohort, age, field, continued, 2))
    for outcome, response, positive in [
        ("experience", 0.62 if domestic else 0.51, 0.77 if domestic else 0.73),
        ("employment", 0.44 if domestic else 0.36, 0.72 if domestic else 0.63),
    ]:
        if rng.random() < response:
            surveys.append((sid, outcome, int(rng.random() < positive)))

header = "student_id,cohort,age_band,field,continued,revision"
write_csv("students_batch_1.csv", header, students + [students[1], ("", "Domestic", "25+", "Business", 1, 1)])
write_csv("students_batch_2.csv", header, updates + [updates[0], students[1]])
write_csv("survey.csv", "student_id,outcome,result", surveys + [surveys[0], ("UNMATCHED", "experience", 1)])

CONFIG = '''import re

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "education_cohort_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in [catalog, schema]):
    raise ValueError("Set catalog to a writable Unity Catalog catalog and use a simple schema name.")
namespace = f"`{catalog}`.`{schema}`"

def table(name):
    return f"{namespace}.`{name}`"
'''


def notebook(name, *cells):
    (ROOT / "notebooks" / (name + ".py")).write_text(
        "# Databricks notebook source\n" + "\n\n# COMMAND ----------\n\n".join(cells) + "\n",
        encoding="utf-8",
    )


notebook("00_setup", CONFIG, '''spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {table('incoming')}")
print(f"/Volumes/{catalog}/{schema}/incoming")
''')
notebook("01_ingest", CONFIG, '''from pyspark.sql import functions as F

dbutils.widgets.dropdown("batch", "1", ["1", "2"])
batch = int(dbutils.widgets.get("batch"))
path = f"/Volumes/{catalog}/{schema}/incoming"
students = None
for number in range(1, batch + 1):
    incoming = spark.read.option("header", True).csv(f"{path}/students_batch_{number}.csv").withColumn("batch", F.lit(number))
    students = incoming if students is None else students.unionByName(incoming)

survey = spark.read.option("header", True).csv(f"{path}/survey.csv")
for name, frame in [("bronze_students", students), ("bronze_survey", survey)]:
    frame.write.format("delta").mode("overwrite").saveAsTable(table(name))
''')

queries = (ROOT / "queries.py").read_text()
notebook("02_transform_report", CONFIG, queries, '''def render(query):
    return query.format(**{name: table(name) for name in (
        "bronze_students", "bronze_survey", "silver_students", "silver_survey", "gold_outcomes"
    )})

for name, query in [
    ("quarantine_students", REJECTED),
    ("silver_students", STUDENTS),
    ("silver_survey", SURVEY),
    ("gold_outcomes", METRICS),
]:
    spark.sql(f"CREATE OR REPLACE TABLE {table(name)} USING DELTA AS {render(query)}")

display(spark.sql(f"SELECT * FROM {table('gold_outcomes')} ORDER BY outcome, cohort, age_band"))
''')
notebook("03_analyse", CONFIG, (ROOT / "statistics_core.py").read_text(), '''rows = [row.asDict() for row in spark.table(table("gold_outcomes")).collect()]
for outcome in ("continuation", "experience", "employment"):
    print(outcome, standardised_gap([row for row in rows if row["outcome"] == outcome]))

# This is a first disclosure check. Before release, also review complementary
# suppression and what readers could infer across tables and repeated releases.
display(spark.table(table("gold_outcomes")).where("eligible >= 10 AND observed >= 10 AND positive >= 10 AND observed - positive >= 10"))
''')
