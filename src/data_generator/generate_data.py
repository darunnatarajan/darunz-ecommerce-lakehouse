# Databricks notebook source
# MAGIC %md
# MAGIC # Raw data generator — Darunz E-commerce Lakehouse
# MAGIC
# MAGIC Every run drops **one new batch** of raw files into the landing volume, the way an
# MAGIC e-commerce platform's exports would arrive:
# MAGIC
# MAGIC | Entity | Format | Behaviour per batch |
# MAGIC |---|---|---|
# MAGIC | `customers` | CSV | 1st batch: full load (500). Later: new sign-ups + profile updates (CDC-style, `updated_at`) |
# MAGIC | `products` | CSV | 1st batch: full catalog (120). Later: a few price changes |
# MAGIC | `orders` | JSON lines | New orders with nested `items` array |
# MAGIC | `clickstream` | JSON lines | Web/app events: page_view → product_view → add_to_cart → checkout → purchase |
# MAGIC
# MAGIC It also injects **realistic dirt** on purpose so the Silver layer has real work to do:
# MAGIC duplicates, late-arriving orders, null keys, negative quantities, unknown statuses.
# MAGIC
# MAGIC Pure Python standard library only — runs on serverless / Free Edition with no installs.

# COMMAND ----------

dbutils.widgets.text("landing_path", "/Volumes/workspace/ecommerce/landing")
dbutils.widgets.text("num_orders", "300")
dbutils.widgets.text("num_events", "3000")

LANDING = dbutils.widgets.get("landing_path").rstrip("/")
NUM_ORDERS = int(dbutils.widgets.get("num_orders"))
NUM_EVENTS = int(dbutils.widgets.get("num_events"))

# COMMAND ----------

import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
NOW = datetime.now(IST)
STATE_FILE = f"{LANDING}/_state/state.json"

for sub in ["customers", "products", "orders", "clickstream", "_state"]:
    os.makedirs(f"{LANDING}/{sub}", exist_ok=True)

# Generator state survives between runs so IDs keep growing like a real system.
if os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        state = json.load(f)
else:
    state = {"batch_no": 0, "next_customer_id": 1, "next_order_id": 100001, "catalog": {}, "prices": {}}

state["batch_no"] += 1
BATCH = state["batch_no"]
STAMP = NOW.strftime("%Y%m%d_%H%M%S")
random.seed(BATCH * 7919)

print(f"Batch {BATCH} -> {LANDING}")

# COMMAND ----------

# MAGIC %md ## Reference data

# COMMAND ----------

CITIES = [
    ("Chennai", "Tamil Nadu"), ("Bengaluru", "Karnataka"), ("Hyderabad", "Telangana"),
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"), ("Coimbatore", "Tamil Nadu"), ("Kochi", "Kerala"),
    ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"), ("Madurai", "Tamil Nadu"),
]
FIRST = ["Arun", "Priya", "Karthik", "Divya", "Rahul", "Sneha", "Vijay", "Anitha", "Suresh",
         "Meena", "Arjun", "Kavya", "Ravi", "Lakshmi", "Manoj", "Deepa", "Sanjay", "Pooja",
         "Ganesh", "Nithya", "Ajay", "Swathi", "Harish", "Keerthi"]
LAST = ["Kumar", "Sharma", "Iyer", "Reddy", "Nair", "Patel", "Singh", "Rao", "Menon",
        "Gupta", "Das", "Pillai", "Joshi", "Krishnan"]
SEGMENTS = ["new", "regular", "premium"]
CATEGORIES = {
    "Electronics": (999, 79999), "Fashion": (299, 4999), "Home & Kitchen": (199, 14999),
    "Books": (99, 1499), "Beauty": (149, 2999), "Sports": (399, 19999), "Grocery": (49, 999),
}
BRANDS = ["Zenith", "UrbanLeaf", "Kraftwell", "Nova", "BlueOrbit", "Sahara", "Mithra", "Pixel"]
PAYMENTS = ["UPI", "UPI", "UPI", "Card", "NetBanking", "COD", "Wallet"]
STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled", "returned"]
DEVICES = ["android", "ios", "web-desktop", "web-mobile"]
REFERRERS = ["google", "instagram", "direct", "email", "youtube", "affiliate"]

# COMMAND ----------

# MAGIC %md ## Products (CSV)

# COMMAND ----------

def write_csv(path, rows, header):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

product_header = ["product_id", "product_name", "category", "brand", "list_price", "is_active", "updated_at"]
product_rows = []

if BATCH == 1:
    pid = 1
    for cat, (lo, hi) in CATEGORIES.items():
        for _ in range(120 // len(CATEGORIES) + 1):
            if pid > 120:
                break
            brand = random.choice(BRANDS)
            row = {
                "product_id": f"P{pid:04d}",
                "product_name": f"{brand} {cat.split(' ')[0]} Item {pid}",
                "category": cat, "brand": brand,
                "list_price": round(random.uniform(lo, hi), 0),
                "is_active": True, "updated_at": NOW.isoformat(),
            }
            state["catalog"][row["product_id"]] = row
            product_rows.append(row)
            pid += 1
else:
    # A handful of full-row price changes per batch -> perfect for SCD Type 2 later
    for p in random.sample(list(state["catalog"].keys()), k=5):
        row = dict(state["catalog"][p])
        row["list_price"] = round(row["list_price"] * random.uniform(0.85, 1.15), 0)
        row["updated_at"] = NOW.isoformat()
        state["catalog"][p] = row
        product_rows.append(row)

state["prices"] = {p: r["list_price"] for p, r in state["catalog"].items()}

write_csv(f"{LANDING}/products/products_{BATCH:05d}_{STAMP}.csv", product_rows, product_header)
print(f"products: {len(product_rows)} rows")

# COMMAND ----------

# MAGIC %md ## Customers (CSV, CDC-style)

# COMMAND ----------

def new_customer(cid):
    first, last = random.choice(FIRST), random.choice(LAST)
    city, st = random.choice(CITIES)
    return {
        "customer_id": f"C{cid:06d}",
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
        "phone": f"+91{random.randint(6000000000, 9999999999)}",
        "city": city, "state": st,
        "segment": random.choice(SEGMENTS),
        "signup_date": (NOW - timedelta(days=random.randint(0, 900))).date().isoformat(),
        "updated_at": NOW.isoformat(),
    }

customer_header = ["customer_id", "full_name", "email", "phone", "city", "state", "segment", "signup_date", "updated_at"]
customer_rows = []
n_new = 500 if BATCH == 1 else random.randint(15, 30)
state.setdefault("customers", {})
existing_ids = list(state["customers"].keys())

for _ in range(n_new):
    c = new_customer(state["next_customer_id"])
    state["customers"][c["customer_id"]] = c
    customer_rows.append(c)
    state["next_customer_id"] += 1

if BATCH > 1:
    # Existing customers who moved city or changed segment (full row, newer updated_at)
    for cid in random.sample(existing_ids, k=min(len(existing_ids), random.randint(5, 12))):
        c = dict(state["customers"][cid])
        if random.random() < 0.5:
            c["city"], c["state"] = random.choice(CITIES)
        else:
            c["segment"] = random.choice(SEGMENTS)
        c["updated_at"] = NOW.isoformat()
        state["customers"][cid] = c
        customer_rows.append(c)

write_csv(f"{LANDING}/customers/customers_{BATCH:05d}_{STAMP}.csv", customer_rows, customer_header)
print(f"customers: {len(customer_rows)} rows")

# COMMAND ----------

# MAGIC %md ## Orders (JSON lines, nested items, intentional data-quality issues)

# COMMAND ----------

max_cid = state["next_customer_id"] - 1
product_ids = list(state["prices"].keys())
orders = []

for _ in range(NUM_ORDERS):
    oid = state["next_order_id"]
    state["next_order_id"] += 1
    city, st = random.choice(CITIES)
    order_ts = NOW - timedelta(minutes=random.randint(0, 24 * 60))
    if random.random() < 0.03:                      # late-arriving order (up to 3 days old)
        order_ts -= timedelta(days=random.randint(1, 3))

    items = []
    for p in random.sample(product_ids, k=random.randint(1, 4)):
        items.append({"product_id": p, "quantity": random.randint(1, 3), "unit_price": state["prices"][p]})

    order = {
        "order_id": f"O{oid}",
        "customer_id": f"C{random.randint(1, max_cid):06d}",
        "order_ts": order_ts.isoformat(),
        "status": random.choices(STATUSES, weights=[20, 25, 25, 20, 6, 4])[0],
        "payment_method": random.choice(PAYMENTS),
        "coupon_code": random.choice([None, None, None, "FEST10", "NEWUSER", "FLAT200"]),
        "shipping": {"city": city, "state": st, "pincode": str(random.randint(110001, 699999))},
        "items": items,
        "source_system": "shop-api-v2",
    }

    r = random.random()                             # ~2% dirty records
    if r < 0.005:
        order["customer_id"] = None
    elif r < 0.010:
        order["items"][0]["quantity"] = -1
    elif r < 0.015:
        order["status"] = "UNKNOWN_STATUS"
    elif r < 0.020:
        order["order_ts"] = "not-a-timestamp"

    orders.append(order)
    if random.random() < 0.01:                      # ~1% duplicates (API retry)
        orders.append(order)

with open(f"{LANDING}/orders/orders_{BATCH:05d}_{STAMP}.json", "w") as f:
    for o in orders:
        f.write(json.dumps(o) + "\n")
print(f"orders: {len(orders)} rows")

# COMMAND ----------

# MAGIC %md ## Clickstream (JSON lines)

# COMMAND ----------

FUNNEL = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]
events = []
while len(events) < NUM_EVENTS:
    session = str(uuid.uuid4())
    customer = None if random.random() < 0.3 else f"C{random.randint(1, max_cid):06d}"
    device, ref = random.choice(DEVICES), random.choice(REFERRERS)
    t = NOW - timedelta(minutes=random.randint(0, 24 * 60))
    depth = random.choices([1, 2, 3, 4, 5], weights=[30, 35, 18, 9, 8])[0]   # realistic drop-off
    product = random.choice(product_ids)
    for step in FUNNEL[:depth]:
        t += timedelta(seconds=random.randint(5, 240))
        events.append({
            "event_id": str(uuid.uuid4()),
            "session_id": session,
            "customer_id": customer,
            "event_type": step,
            "product_id": None if step == "page_view" else product,
            "event_ts": t.isoformat(),
            "device": device,
            "referrer": ref,
            "page_url": "/" if step == "page_view" else f"/p/{product.lower()}",
        })

with open(f"{LANDING}/clickstream/clickstream_{BATCH:05d}_{STAMP}.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")
print(f"clickstream: {len(events)} rows")

# COMMAND ----------

with open(STATE_FILE, "w") as f:
    json.dump(state, f)

dbutils.notebook.exit(json.dumps({
    "batch": BATCH, "customers": len(customer_rows), "products": len(product_rows),
    "orders": len(orders), "events": len(events),
}))
