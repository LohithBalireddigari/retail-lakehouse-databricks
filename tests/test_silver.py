import datetime as dt
from decimal import Decimal

from retail_lakehouse import bronze, silver
from retail_lakehouse.schemas import CUSTOMERS_RAW, ORDER_ITEMS_RAW, ORDERS_RAW, PRODUCTS_RAW


def _bronze(spark, rows, schema, source):
    return bronze.add_ingestion_metadata(spark.createDataFrame(rows, schema), source)


def test_clean_customers_dedupes_and_normalises(spark):
    rows = [
        ("C1", "  jane ", "doe", "Jane.Doe@Example.com", "austin", "tx", "2024-01-05", "2024-06-01 10:00:00"),
        ("C1", "Jane", "Doe", "old@example.com", "Austin", "TX", "2024-01-05", "2024-01-01 10:00:00"),
        ("C2", "Bob", "Lee", "not-an-email", "Dallas", "TX", "01/15/2024", "2024-02-01 09:00:00"),
    ]
    valid, quarantine = silver.clean_customers(_bronze(spark, rows, CUSTOMERS_RAW, "customers"))

    out = valid.collect()
    assert len(out) == 1
    c1 = out[0]
    assert (c1.first_name, c1.email, c1.state, c1.city) == ("Jane", "jane.doe@example.com", "TX", "Austin")
    assert c1.signup_date == dt.date(2024, 1, 5)

    q = quarantine.collect()
    assert len(q) == 1 and q[0].customer_id == "C2"
    assert q[0]._failed_rules == ["email_valid"]


def test_clean_customers_parses_us_date_format(spark):
    rows = [("C9", "A", "B", "a@b.com", "X", "CA", "03/07/2024", "2024-03-07 00:00:00")]
    valid, _ = silver.clean_customers(_bronze(spark, rows, CUSTOMERS_RAW, "customers"))
    assert valid.collect()[0].signup_date == dt.date(2024, 3, 7)


def test_clean_products_rejects_bad_prices(spark):
    rows = [
        ("P1", "Lamp", "home", "Acme", "19.99", "true"),
        ("P2", "Mug", "home", "Acme", "0", "true"),
        ("P3", "Cup", "home", "Acme", "N/A", "false"),
    ]
    valid, quarantine = silver.clean_products(_bronze(spark, rows, PRODUCTS_RAW, "products"))
    v = valid.collect()
    assert [r.product_id for r in v] == ["P1"]
    assert v[0].unit_price == Decimal("19.99") and v[0].category == "Home" and v[0].is_active is True
    assert sorted(r.product_id for r in quarantine.collect()) == ["P2", "P3"]


def test_clean_orders_parses_both_timestamp_formats(spark):
    rows = [
        ("O1", "C1", "2025-03-01T14:30:00", "completed", "WEB", "PayPal"),
        ("O2", "C1", "2025-03-02 09:00:00", "SHIPPED", "store", "credit_card"),
        ("O3", "C2", "2025-03-02 09:00:00", "LOST", "web", "paypal"),
        ("O4", None, "2025-03-02 09:00:00", "COMPLETED", "web", "paypal"),
    ]
    valid, quarantine = silver.clean_orders(_bronze(spark, rows, ORDERS_RAW, "orders"))
    v = {r.order_id: r for r in valid.collect()}
    assert set(v) == {"O1", "O2"}
    assert v["O1"].status == "COMPLETED" and v["O1"].channel == "web"
    assert v["O1"].order_date == dt.date(2025, 3, 1)
    q = {r.order_id: r._failed_rules for r in quarantine.collect()}
    assert q == {"O3": ["status_known"], "O4": ["customer_id_not_null"]}


def test_clean_order_items_computes_line_total(spark):
    rows = [
        ("I1", "O1", "P1", "2", "10.00", "0.1"),
        ("I2", "O1", "P1", "-1", "10.00", "0"),
        ("I3", "O1", "P1", "1", "10.00", "1.5"),
        ("I4", "O1", "P1", "3", "5.00", None),
    ]
    valid, quarantine = silver.clean_order_items(_bronze(spark, rows, ORDER_ITEMS_RAW, "order_items"))
    v = {r.order_item_id: r for r in valid.collect()}
    assert v["I1"].line_total == Decimal("18.00")
    assert v["I4"].line_total == Decimal("15.00")
    q = {r.order_item_id: r._failed_rules for r in quarantine.collect()}
    assert q == {"I2": ["quantity_positive"], "I3": ["discount_in_range"]}


def test_drop_orphans(spark):
    items = spark.createDataFrame([("I1", "O1", "P1"), ("I2", "O9", "P1"), ("I3", "O1", "P9")],
                                  "order_item_id STRING, order_id STRING, product_id STRING")
    orders = spark.createDataFrame([("O1",)], "order_id STRING")
    products = spark.createDataFrame([("P1",)], "product_id STRING")
    valid, quarantine = silver.drop_orphans(items, orders, products)
    assert [r.order_item_id for r in valid.collect()] == ["I1"]
    q = {r.order_item_id: r._failed_rules for r in quarantine.collect()}
    assert q == {"I2": ["order_exists"], "I3": ["product_exists"]}
