"""Generate a realistic (and deliberately messy) retail dataset.

Pure standard library, so it runs both locally and inside a Databricks
notebook writing straight into a Unity Catalog volume (``/Volumes/...``).

Dirty data injected on purpose, to exercise the Silver quality rules:
  * duplicate customers with older ``updated_at``
  * invalid / missing emails, mixed date formats, stray whitespace
  * products with zero or non-numeric prices
  * orders with unknown status or missing customer
  * order items with negative quantity or out-of-range discount
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David",
               "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas",
               "Sarah", "Priya", "Arjun", "Wei", "Mei", "Carlos", "Sofia", "Ahmed", "Fatima"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
              "Martinez", "Patel", "Reddy", "Chen", "Wang", "Kim", "Nguyen", "Lopez", "Wilson", "Khan"]
CITIES = [("Austin", "TX"), ("Dallas", "TX"), ("Seattle", "WA"), ("Denver", "CO"), ("Chicago", "IL"),
          ("New York", "NY"), ("Boston", "MA"), ("Atlanta", "GA"), ("Phoenix", "AZ"), ("San Jose", "CA"),
          ("Los Angeles", "CA"), ("Miami", "FL"), ("Charlotte", "NC"), ("Columbus", "OH")]
CATEGORIES = {
    "Electronics": (["Headphones", "Smartwatch", "Bluetooth Speaker", "Tablet", "Webcam", "Charger"], (19, 499)),
    "Home": (["Coffee Maker", "Air Purifier", "Desk Lamp", "Blender", "Vacuum"], (25, 349)),
    "Apparel": (["Running Shoes", "Rain Jacket", "Hoodie", "Jeans", "Backpack"], (15, 180)),
    "Beauty": (["Face Serum", "Shampoo", "Sunscreen", "Perfume"], (8, 120)),
    "Sports": (["Yoga Mat", "Dumbbells", "Water Bottle", "Bike Helmet", "Tennis Racket"], (10, 220)),
    "Grocery": (["Organic Coffee", "Protein Bars", "Olive Oil", "Green Tea"], (4, 45)),
}
BRANDS = ["Northwind", "Acme", "Contoso", "Lumen", "Apex", "Evergreen", "Bluepeak", "Orbit"]
STATUSES = ["COMPLETED"] * 70 + ["SHIPPED"] * 12 + ["PENDING"] * 6 + ["CANCELLED"] * 7 + ["RETURNED"] * 5
CHANNELS = ["web"] * 55 + ["mobile_app"] * 35 + ["store"] * 10
PAYMENTS = ["credit_card", "debit_card", "paypal", "apple_pay", "gift_card"]


def _maybe(rng: random.Random, p: float) -> bool:
    return rng.random() < p


def generate(output_dir: str, n_customers: int = 800, n_products: int = 150, n_orders: int = 6000,
             start: str = "2024-08-01", end: str = "2025-07-31", seed: int = 42) -> dict:
    rng = random.Random(seed)
    os.makedirs(output_dir, exist_ok=True)
    start_dt = datetime.fromisoformat(start)
    span_days = (datetime.fromisoformat(end) - start_dt).days

    # ---- customers ----------------------------------------------------
    customers = []
    for i in range(1, n_customers + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES)
        signup = start_dt - timedelta(days=rng.randint(0, 900))
        email = f"{first}.{last}{i}@example.com".lower()
        if _maybe(rng, 0.02):
            email = email.replace("@", "")          # invalid email
        elif _maybe(rng, 0.01):
            email = ""                              # missing email
        signup_str = signup.strftime("%m/%d/%Y") if _maybe(rng, 0.15) else signup.strftime("%Y-%m-%d")
        updated = signup + timedelta(days=rng.randint(0, 300))
        customers.append([f"C{i:05d}", f"  {first} " if _maybe(rng, 0.05) else first, last, email,
                          city, state.lower() if _maybe(rng, 0.05) else state, signup_str,
                          updated.strftime("%Y-%m-%d %H:%M:%S")])
        if _maybe(rng, 0.03):  # older duplicate record -> should be dropped by dedupe
            dup = list(customers[-1])
            dup[3] = f"old.{email}" if email else "old@example.com"
            dup[7] = (updated - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            customers.append(dup)

    # ---- products -----------------------------------------------------
    products, prices = [], {}
    for i in range(1, n_products + 1):
        category = rng.choice(list(CATEGORIES))
        names, (lo, hi) = CATEGORIES[category]
        brand = rng.choice(BRANDS)
        price = round(rng.uniform(lo, hi), 2)
        prices[f"P{i:04d}"] = price
        price_str = str(price)
        if _maybe(rng, 0.01):
            price_str = "0"
        elif _maybe(rng, 0.01):
            price_str = "N/A"
        products.append([f"P{i:04d}", f"{brand} {rng.choice(names)}", category, brand, price_str,
                         rng.choice(["true", "true", "true", "false"])])

    # ---- orders + items -----------------------------------------------
    orders, items, item_id = [], [], 1
    for i in range(1, n_orders + 1):
        # mild seasonality: more orders in Nov/Dec
        day = rng.randint(0, span_days)
        ts = start_dt + timedelta(days=day, seconds=rng.randint(0, 86399))
        if ts.month in (11, 12) and _maybe(rng, 0.3):
            ts = ts.replace(hour=rng.randint(18, 23))
        status = rng.choice(STATUSES)
        if _maybe(rng, 0.005):
            status = "UNKNOWN"
        customer_id = f"C{rng.randint(1, n_customers):05d}"
        if _maybe(rng, 0.005):
            customer_id = None
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S") if _maybe(rng, 0.5) else ts.strftime("%Y-%m-%d %H:%M:%S")
        orders.append({"order_id": f"O{i:06d}", "customer_id": customer_id, "order_ts": ts_str,
                       "status": status.lower() if _maybe(rng, 0.1) else status,
                       "channel": rng.choice(CHANNELS), "payment_method": rng.choice(PAYMENTS)})

        for _ in range(rng.choices([1, 2, 3, 4], weights=[45, 30, 17, 8])[0]):
            pid = f"P{rng.randint(1, n_products):04d}"
            qty = rng.choices([1, 2, 3, 5], weights=[70, 20, 8, 2])[0]
            if _maybe(rng, 0.004):
                qty = -1
            discount = rng.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2])
            if _maybe(rng, 0.003):
                discount = 1.5
            items.append([f"OI{item_id:07d}", f"O{i:06d}", pid, qty, prices[pid], discount])
            item_id += 1

    # ---- write --------------------------------------------------------
    def write_csv(name, header, rows):
        folder = os.path.join(output_dir, name)
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, f"{name}.csv"), "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)

    write_csv("customers", ["customer_id", "first_name", "last_name", "email", "city", "state",
                            "signup_date", "updated_at"], customers)
    write_csv("products", ["product_id", "product_name", "category", "brand", "unit_price", "is_active"], products)
    write_csv("order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
                              "discount_pct"], items)
    os.makedirs(os.path.join(output_dir, "orders"), exist_ok=True)
    with open(os.path.join(output_dir, "orders", "orders.json"), "w") as fh:
        for o in orders:
            fh.write(json.dumps(o) + "\n")

    return {"customers": len(customers), "products": len(products), "orders": len(orders),
            "order_items": len(items)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample retail data")
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--orders", type=int, default=6000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    counts = generate(args.output, n_orders=args.orders, seed=args.seed)
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
