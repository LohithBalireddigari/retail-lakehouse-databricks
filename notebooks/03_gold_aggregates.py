# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold aggregates
# MAGIC Builds the sales fact table and the KPI tables that feed the dashboard.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")

import sys, os
sys.path.append(os.path.abspath("../src"))

from retail_lakehouse import gold
from retail_lakehouse.config import LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
s = lambda e: spark.table(cfg.table("silver", e))

# COMMAND ----------

fact = gold.build_fact_sales(s("order_items"), s("orders"), s("products"), s("customers"))
(fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .partitionBy("order_date").saveAsTable(cfg.table("gold", "fact_sales")))
fact = spark.table(cfg.table("gold", "fact_sales"))

# COMMAND ----------

outputs = {
    "daily_revenue": gold.daily_revenue(fact),
    "customer_ltv": gold.customer_lifetime_value(fact),
    "product_performance": gold.product_performance(fact),
    "channel_mix": gold.channel_mix(fact),
}
for name, df in outputs.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("gold", name))
    print(f"gold_{name}: {spark.table(cfg.table('gold', name)).count():,} rows")

# COMMAND ----------

display(spark.table(cfg.table("gold", "daily_revenue")).orderBy("order_date"))

# COMMAND ----------

display(spark.table(cfg.table("gold", "customer_ltv")).groupBy("segment").count())
