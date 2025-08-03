# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver transform
# MAGIC Types, cleans and de-duplicates each entity, applies data-quality rules,
# MAGIC and writes failing rows to `quarantine_*` tables with the rules they broke.
# MAGIC Customers are upserted with `MERGE` so reruns are idempotent.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")

import sys, os
sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
from retail_lakehouse import silver
from retail_lakehouse.config import LakehouseConfig

cfg = LakehouseConfig(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
bronze = lambda e: spark.table(cfg.table("bronze", e))

# COMMAND ----------

def write(df, layer, entity, mode="overwrite"):
    (df.write.format("delta").mode(mode).option("overwriteSchema", "true")
       .saveAsTable(cfg.table(layer, entity)))

customers, q_customers = silver.clean_customers(bronze("customers"))
products, q_products = silver.clean_products(bronze("products"))
orders, q_orders = silver.clean_orders(bronze("orders"))
items, q_items = silver.clean_order_items(bronze("order_items"))
items, q_orphans = silver.drop_orphans(items, orders, products)

# COMMAND ----------

# MAGIC %md ### Customers: MERGE (SCD type 1 upsert)

# COMMAND ----------

target = cfg.table("silver", "customers")
if spark.catalog.tableExists(target):
    (DeltaTable.forName(spark, target).alias("t")
        .merge(customers.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())
else:
    write(customers, "silver", "customers")

# COMMAND ----------

write(products, "silver", "products")
write(orders, "silver", "orders")
write(items, "silver", "order_items")

write(q_customers, "quarantine", "customers")
write(q_products, "quarantine", "products")
write(q_orders, "quarantine", "orders")
write(q_items.unionByName(q_orphans, allowMissingColumns=True), "quarantine", "order_items")

# COMMAND ----------

spark.sql(f"OPTIMIZE {cfg.table('silver', 'order_items')} ZORDER BY (order_id)")
spark.sql(f"OPTIMIZE {cfg.table('silver', 'orders')} ZORDER BY (order_date)")

# COMMAND ----------

for e in ["customers", "products", "orders", "order_items"]:
    print(f"{e:12s} silver={spark.table(cfg.table('silver', e)).count():>7,}  "
          f"quarantine={spark.table(cfg.table('quarantine', e)).count():>5,}")
