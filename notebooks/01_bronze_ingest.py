# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest
# MAGIC Incrementally loads raw files from the volume with **Auto Loader** into Bronze Delta tables.
# MAGIC Every value stays a string; typing happens in Silver so nothing is silently lost.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")

import sys, os
sys.path.append(os.path.abspath("../src"))

from retail_lakehouse.bronze import read_raw_stream
from retail_lakehouse.config import ENTITIES, LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))

# COMMAND ----------

for entity in ENTITIES:
    target = cfg.table("bronze", entity)
    (
        read_raw_stream(spark, entity, f"{cfg.raw_path}/{entity}", f"{cfg.checkpoint_path}/{entity}/schema")
        .writeStream.format("delta")
        .option("checkpointLocation", f"{cfg.checkpoint_path}/{entity}/bronze")
        .trigger(availableNow=True)
        .toTable(target)
        .awaitTermination()
    )
    print(f"{target}: {spark.table(target).count():,} rows")

# COMMAND ----------

display(spark.table(cfg.table("bronze", "orders")).limit(10))
