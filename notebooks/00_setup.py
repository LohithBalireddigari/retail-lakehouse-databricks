# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog schema + volume and lands the sample raw data.
# MAGIC Works on **Databricks Free Edition** (serverless).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
dbutils.widgets.text("orders", "6000")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
n_orders = int(dbutils.widgets.get("orders"))

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

from retail_lakehouse.config import LakehouseConfig
from retail_lakehouse.data_generator import generate

cfg = LakehouseConfig(catalog=catalog, schema=schema)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{cfg.volume}")

# COMMAND ----------

counts = generate(cfg.raw_path, n_orders=n_orders)
display(spark.createDataFrame([counts]))

# COMMAND ----------

display(dbutils.fs.ls(cfg.raw_path))
