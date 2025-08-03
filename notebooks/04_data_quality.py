# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality report
# MAGIC Summarises pass rates per entity and which rules failed most. Fails the job
# MAGIC if any entity drops below the configured threshold.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
dbutils.widgets.text("min_pass_rate", "0.90")

import sys, os
sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F
from retail_lakehouse import quality
from retail_lakehouse.config import ENTITIES, LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
threshold = float(dbutils.widgets.get("min_pass_rate"))

# COMMAND ----------

reports = [
    quality.build_report(e, spark.table(cfg.table("silver", e)), spark.table(cfg.table("quarantine", e))).as_dict()
    for e in ENTITIES
]
report_df = spark.createDataFrame(reports).withColumn("checked_at", F.current_timestamp())
report_df.write.format("delta").mode("append").saveAsTable(cfg.table("gold", "data_quality"))
display(report_df)

# COMMAND ----------

for e in ENTITIES:
    print(e)
    display(quality.rule_failure_counts(spark.table(cfg.table("quarantine", e))))

# COMMAND ----------

failing = [r for r in reports if r["pass_rate"] < threshold]
assert not failing, f"Data quality below {threshold:.0%}: {failing}"
print("All entities passed the data-quality threshold.")
