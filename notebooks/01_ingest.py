# Databricks notebook source
import re

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "education_cohort_lab")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in [catalog, schema]):
    raise ValueError("Set catalog to a writable Unity Catalog catalog and use a simple schema name.")
namespace = f"`{catalog}`.`{schema}`"

def table(name):
    return f"{namespace}.`{name}`"


# COMMAND ----------

from pyspark.sql import functions as F

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

