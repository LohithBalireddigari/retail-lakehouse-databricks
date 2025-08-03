"""Explicit schemas for raw landing files.

Raw data is read as strings so that malformed values reach Bronze intact and
are handled (cast or quarantined) in Silver instead of silently becoming null.
"""

from pyspark.sql.types import StringType, StructField, StructType


def _all_strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


CUSTOMERS_RAW = _all_strings(
    "customer_id", "first_name", "last_name", "email", "city", "state", "signup_date", "updated_at"
)

PRODUCTS_RAW = _all_strings("product_id", "product_name", "category", "brand", "unit_price", "is_active")

ORDERS_RAW = _all_strings("order_id", "customer_id", "order_ts", "status", "channel", "payment_method")

ORDER_ITEMS_RAW = _all_strings("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct")

RAW_SCHEMAS = {
    "customers": CUSTOMERS_RAW,
    "products": PRODUCTS_RAW,
    "orders": ORDERS_RAW,
    "order_items": ORDER_ITEMS_RAW,
}
