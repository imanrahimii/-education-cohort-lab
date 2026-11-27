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

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {table('incoming')}")
print(f"/Volumes/{catalog}/{schema}/incoming")

