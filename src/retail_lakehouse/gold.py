"""Gold layer: business-ready fact table and KPI aggregates."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REVENUE_STATUSES = ("COMPLETED", "SHIPPED")


def build_fact_sales(items: DataFrame, orders: DataFrame, products: DataFrame, customers: DataFrame) -> DataFrame:
    """One row per order line, enriched with order, product and customer attributes."""
    return (
        items.alias("i")
        .join(orders.alias("o"), "order_id")
        .join(products.alias("p"), "product_id")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .select(
            "i.order_item_id",
            "i.order_id",
            "o.order_date",
            "o.order_ts",
            "o.status",
            "o.channel",
            "o.payment_method",
            F.col("o.customer_id").alias("customer_id"),
            F.col("c.state").alias("customer_state"),
            "i.product_id",
            "p.product_name",
            "p.category",
            "p.brand",
            "i.quantity",
            "i.unit_price",
            "i.discount_pct",
            "i.line_total",
            F.col("o.status").isin(*REVENUE_STATUSES).alias("is_revenue"),
        )
    )


def _revenue_only(fact: DataFrame) -> DataFrame:
    return fact.filter(F.col("is_revenue"))


def daily_revenue(fact: DataFrame) -> DataFrame:
    """Daily orders, units, revenue and average order value (revenue statuses only)."""
    return (
        _revenue_only(fact)
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("line_total"), 2).alias("revenue"),
        )
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
        .withColumn(
            "revenue_7d_avg",
            F.round(
                F.avg("revenue").over(Window.orderBy("order_date").rowsBetween(-6, 0)), 2
            ),
        )
        .orderBy("order_date")
    )


def customer_lifetime_value(fact: DataFrame, as_of_col: str = "order_date") -> DataFrame:
    """Per-customer LTV with simple RFM-style segmentation."""
    as_of = _revenue_only(fact).agg(F.max(as_of_col)).collect()[0][0]
    per_customer = (
        _revenue_only(fact)
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("line_total"), 2).alias("lifetime_revenue"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn("days_since_last_order", F.datediff(F.lit(as_of), F.col("last_order_date")))
        .withColumn("avg_order_value", F.round(F.col("lifetime_revenue") / F.col("total_orders"), 2))
    )
    return per_customer.withColumn(
        "segment",
        F.when((F.col("total_orders") >= 5) & (F.col("days_since_last_order") <= 60), "Champion")
        .when(F.col("total_orders") >= 3, "Loyal")
        .when(F.col("days_since_last_order") > 180, "At Risk")
        .otherwise("Developing"),
    )


def product_performance(fact: DataFrame) -> DataFrame:
    """Revenue and units per product, ranked within its category."""
    w = Window.partitionBy("category").orderBy(F.desc("revenue"))
    return (
        _revenue_only(fact)
        .groupBy("category", "product_id", "product_name", "brand")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
        .withColumn("rank_in_category", F.dense_rank().over(w))
        .orderBy("category", "rank_in_category")
    )


def channel_mix(fact: DataFrame) -> DataFrame:
    """Monthly revenue share per sales channel."""
    monthly = (
        _revenue_only(fact)
        .withColumn("month", F.date_trunc("month", "order_date").cast("date"))
        .groupBy("month", "channel")
        .agg(F.round(F.sum("line_total"), 2).alias("revenue"))
    )
    total = Window.partitionBy("month")
    return monthly.withColumn(
        "revenue_share", F.round(F.col("revenue") / F.sum("revenue").over(total), 4)
    ).orderBy("month", "channel")
