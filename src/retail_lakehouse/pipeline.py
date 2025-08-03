"""Run the whole Bronze -> Silver -> Gold pipeline.

On Databricks this writes Delta tables to Unity Catalog. Locally
(``python -m retail_lakehouse.pipeline --local``) it writes Parquet to
``./output`` so the full flow can be checked without a cluster.
"""

import argparse
import json
import os
import sys
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from . import bronze, gold, quality, silver
from .config import ENTITIES, LakehouseConfig


def run(spark: SparkSession, raw_path: str) -> Dict[str, DataFrame]:
    """Build every table in memory and return them keyed by ``layer_name``."""
    tables: Dict[str, DataFrame] = {}
    reports = []

    raw = {e: bronze.read_raw(spark, e, f"{raw_path}/{e}") for e in ENTITIES}
    for e, df in raw.items():
        tables[f"bronze_{e}"] = df

    cleaners = {
        "customers": silver.clean_customers,
        "products": silver.clean_products,
        "orders": silver.clean_orders,
        "order_items": silver.clean_order_items,
    }
    clean = {}
    for e, fn in cleaners.items():
        valid, quarantine = fn(raw[e])
        clean[e] = valid.cache()
        tables[f"quarantine_{e}"] = quarantine

    items, orphan_items = silver.drop_orphans(clean["order_items"], clean["orders"], clean["products"])
    clean["order_items"] = items
    tables["quarantine_order_items"] = tables["quarantine_order_items"].unionByName(
        orphan_items, allowMissingColumns=True
    )

    for e in ENTITIES:
        tables[f"silver_{e}"] = clean[e]
        reports.append(quality.build_report(e, clean[e], tables[f"quarantine_{e}"]).as_dict())

    fact = gold.build_fact_sales(clean["order_items"], clean["orders"], clean["products"], clean["customers"]).cache()
    tables["gold_fact_sales"] = fact
    tables["gold_daily_revenue"] = gold.daily_revenue(fact)
    tables["gold_customer_ltv"] = gold.customer_lifetime_value(fact)
    tables["gold_product_performance"] = gold.product_performance(fact)
    tables["gold_channel_mix"] = gold.channel_mix(fact)
    tables["gold_data_quality"] = spark.createDataFrame(reports)
    return tables


def write_delta(tables: Dict[str, DataFrame], cfg: LakehouseConfig) -> None:
    for name, df in tables.items():
        layer, entity = name.split("_", 1)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            cfg.table(layer, entity)
        )


def write_local(tables: Dict[str, DataFrame], out: str) -> None:
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{out}/{name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="run with local Spark, write Parquet")
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--out", default="output")
    args = parser.parse_args()

    if args.local:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[*]").appName("retail-lakehouse")
            .config("spark.sql.shuffle.partitions", "4").getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        tables = run(spark, args.raw)
        write_local(tables, args.out)
        print(json.dumps([r.asDict() for r in tables["gold_data_quality"].collect()], indent=2))
        tables["gold_daily_revenue"].orderBy("order_date", ascending=False).show(5)
        tables["gold_customer_ltv"].groupBy("segment").count().show()
    else:
        spark = SparkSession.builder.getOrCreate()
        cfg = LakehouseConfig()
        write_delta(run(spark, cfg.raw_path), cfg)


if __name__ == "__main__":
    main()
