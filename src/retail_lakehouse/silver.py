"""Silver layer: typed, cleaned, de-duplicated and validated entities.

Every ``clean_*`` function returns ``(valid, quarantine)``.
"""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import quality

Split = Tuple[DataFrame, DataFrame]

_TS_FORMATS = ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss'Z'", "MM/dd/yyyy HH:mm")
_DATE_FORMATS = ("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy")


def parse_timestamp(col: str) -> F.Column:
    """Try several timestamp layouts; return null if none match."""
    return F.coalesce(*[F.expr(f'try_to_timestamp({col}, "{fmt}")') for fmt in _TS_FORMATS])


def parse_date(col: str) -> F.Column:
    return F.coalesce(*[F.expr(f'to_date(try_to_timestamp({col}, "{fmt}"))') for fmt in _DATE_FORMATS])


def _clean_str(col: str) -> F.Column:
    """Trim and turn empty strings into null."""
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def deduplicate(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep the most recent row per key (by ``order_col`` descending)."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_customers(bronze: DataFrame) -> Split:
    typed = bronze.select(
        _clean_str("customer_id").alias("customer_id"),
        F.initcap(_clean_str("first_name")).alias("first_name"),
        F.initcap(_clean_str("last_name")).alias("last_name"),
        F.lower(_clean_str("email")).alias("email"),
        F.initcap(_clean_str("city")).alias("city"),
        F.upper(_clean_str("state")).alias("state"),
        parse_date("signup_date").alias("signup_date"),
        parse_timestamp("updated_at").alias("updated_at"),
        "_ingested_at",
    )
    deduped = deduplicate(typed, "customer_id", "updated_at")
    return quality.split_by_rules(deduped, quality.customer_rules())


def clean_products(bronze: DataFrame) -> Split:
    typed = bronze.select(
        _clean_str("product_id").alias("product_id"),
        _clean_str("product_name").alias("product_name"),
        F.initcap(_clean_str("category")).alias("category"),
        _clean_str("brand").alias("brand"),
        F.expr("try_cast(unit_price AS DECIMAL(10,2))").alias("unit_price"),
        F.lower(_clean_str("is_active")).isin("true", "1", "yes", "y").alias("is_active"),
        "_ingested_at",
    )
    deduped = deduplicate(typed, "product_id", "_ingested_at")
    return quality.split_by_rules(deduped, quality.product_rules())


def clean_orders(bronze: DataFrame) -> Split:
    typed = bronze.select(
        _clean_str("order_id").alias("order_id"),
        _clean_str("customer_id").alias("customer_id"),
        parse_timestamp("order_ts").alias("order_ts"),
        F.upper(_clean_str("status")).alias("status"),
        F.lower(_clean_str("channel")).alias("channel"),
        F.lower(_clean_str("payment_method")).alias("payment_method"),
        "_ingested_at",
    ).withColumn("order_date", F.to_date("order_ts"))
    deduped = deduplicate(typed, "order_id", "_ingested_at")
    return quality.split_by_rules(deduped, quality.order_rules())


def clean_order_items(bronze: DataFrame) -> Split:
    typed = bronze.select(
        _clean_str("order_item_id").alias("order_item_id"),
        _clean_str("order_id").alias("order_id"),
        _clean_str("product_id").alias("product_id"),
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.expr("try_cast(unit_price AS DECIMAL(10,2))").alias("unit_price"),
        F.coalesce(F.expr("try_cast(discount_pct AS DOUBLE)"), F.lit(0.0)).alias("discount_pct"),
        "_ingested_at",
    ).withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2).cast("decimal(12,2)"),
    )
    deduped = deduplicate(typed, "order_item_id", "_ingested_at")
    return quality.split_by_rules(deduped, quality.order_item_rules())


def drop_orphans(items: DataFrame, orders: DataFrame, products: DataFrame) -> Split:
    """Quarantine order items whose order or product failed validation / doesn't exist."""
    known = (
        items.join(orders.select("order_id").withColumn("_has_order", F.lit(True)), "order_id", "left")
        .join(products.select("product_id").withColumn("_has_product", F.lit(True)), "product_id", "left")
    )
    rules = {
        "order_exists": F.col("_has_order"),
        "product_exists": F.col("_has_product"),
    }
    valid, quarantine = quality.split_by_rules(known, rules)
    return valid.drop("_has_order", "_has_product"), quarantine.drop("_has_order", "_has_product")
