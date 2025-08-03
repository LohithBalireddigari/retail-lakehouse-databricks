import datetime as dt
from decimal import Decimal

import pytest

from retail_lakehouse import gold


@pytest.fixture
def fact(spark):
    items = spark.createDataFrame(
        [
            ("I1", "O1", "P1", 2, Decimal("10.00"), 0.0, Decimal("20.00")),
            ("I2", "O1", "P2", 1, Decimal("30.00"), 0.0, Decimal("30.00")),
            ("I3", "O2", "P1", 3, Decimal("10.00"), 0.0, Decimal("30.00")),
            ("I4", "O3", "P2", 4, Decimal("30.00"), 0.0, Decimal("120.00")),
        ],
        "order_item_id STRING, order_id STRING, product_id STRING, quantity INT, "
        "unit_price DECIMAL(10,2), discount_pct DOUBLE, line_total DECIMAL(12,2)",
    )
    orders = spark.createDataFrame(
        [
            ("O1", "C1", dt.datetime(2025, 7, 1, 10), dt.date(2025, 7, 1), "COMPLETED", "web", "paypal"),
            ("O2", "C2", dt.datetime(2025, 7, 1, 12), dt.date(2025, 7, 1), "SHIPPED", "mobile_app", "credit_card"),
            ("O3", "C1", dt.datetime(2025, 7, 2, 9), dt.date(2025, 7, 2), "CANCELLED", "web", "paypal"),
        ],
        "order_id STRING, customer_id STRING, order_ts TIMESTAMP, order_date DATE, status STRING, "
        "channel STRING, payment_method STRING",
    )
    products = spark.createDataFrame(
        [("P1", "Mug", "Home", "Acme"), ("P2", "Lamp", "Home", "Lumen")],
        "product_id STRING, product_name STRING, category STRING, brand STRING",
    )
    customers = spark.createDataFrame([("C1", "TX"), ("C2", "WA")], "customer_id STRING, state STRING")
    return gold.build_fact_sales(items, orders, products, customers)


def test_fact_sales_flags_revenue(fact):
    rows = {r.order_item_id: r for r in fact.collect()}
    assert len(rows) == 4
    assert rows["I1"].is_revenue and rows["I3"].is_revenue
    assert not rows["I4"].is_revenue  # cancelled
    assert rows["I3"].customer_state == "WA"


def test_daily_revenue_excludes_cancelled(fact):
    out = gold.daily_revenue(fact).collect()
    assert len(out) == 1
    day = out[0]
    assert day.order_date == dt.date(2025, 7, 1)
    assert (day.orders, day.units) == (2, 6)
    assert day.revenue == Decimal("80.00")
    assert float(day.avg_order_value) == 40.0


def test_customer_ltv(fact):
    ltv = {r.customer_id: r for r in gold.customer_lifetime_value(fact).collect()}
    assert ltv["C1"].lifetime_revenue == Decimal("50.00")
    assert ltv["C1"].total_orders == 1
    assert ltv["C2"].segment == "Developing"


def test_product_performance_ranks_within_category(fact):
    perf = gold.product_performance(fact).collect()
    ranks = {r.product_id: r.rank_in_category for r in perf}
    assert ranks == {"P1": 1, "P2": 2}  # P1 = 50 revenue, P2 = 30 (cancelled order excluded)


def test_channel_mix_shares_sum_to_one(fact):
    rows = gold.channel_mix(fact).collect()
    assert pytest.approx(sum(float(r.revenue_share) for r in rows)) == 1.0
