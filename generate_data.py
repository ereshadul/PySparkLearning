"""
Generates a deliberately messy/complex dataset for the PySparkLearning project.
Run once: python3 generate_data.py
Outputs land in ./data/ -- upload these to S3 (see README.md).
"""
import json
import random
import csv
from datetime import datetime, timedelta

random.seed(42)

OUT = "data"

# ---------- customers.csv (messy: nulls, dup emails, inconsistent date formats) ----------
first = ["Amina","Rakib","Sara","Tom","Wei","Fatima","Carlos","Nina","Omar","Grace",
         "Liam","Priya","Yusuf","Elena","Noah","Mei","Ibrahim","Zoe","Hassan","Ava"]
last = ["Khan","Islam","Smith","Chen","Ahmed","Garcia","Lopez","Rahman","Novak","Silva"]
domains = ["gmail.com","yahoo.com","outlook.com","corp.com"]
date_formats = ["%Y-%m-%d","%m/%d/%Y","%d-%m-%Y"]

customers = []
for i in range(1, 601):
    fn, ln = random.choice(first), random.choice(last)
    email = f"{fn.lower()}.{ln.lower()}{i%7}@{random.choice(domains)}"
    signup = datetime(2022,1,1) + timedelta(days=random.randint(0, 1200))
    fmt = random.choice(date_formats)
    row = {
        "customer_id": f"C{i:04d}",
        "first_name": fn if random.random() > 0.03 else None,
        "last_name": ln,
        "email": email,
        "signup_date": signup.strftime(fmt),
        "country": random.choice(["US","BD","UK","CA","AE",None]),
        "loyalty_tier": random.choice(["gold","silver","bronze","", "Gold"]),
    }
    customers.append(row)
    if random.random() < 0.05:  # inject duplicate customer (slightly different row)
        dup = dict(row)
        dup["customer_id"] = f"C{i:04d}"
        dup["loyalty_tier"] = row["loyalty_tier"].lower() or "bronze"
        customers.append(dup)

with open(f"{OUT}/customers.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader()
    w.writerows(customers)

# ---------- products.csv ----------
categories = ["Electronics","Home","Grocery","Apparel","Toys","Sports"]
products = []
for i in range(1, 151):
    products.append({
        "product_id": f"P{i:04d}",
        "name": f"Product {i}",
        "category": random.choice(categories),
        "unit_cost": round(random.uniform(2, 300), 2),
        "unit_price": round(random.uniform(5, 500), 2),
    })
with open(f"{OUT}/products.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=products[0].keys())
    w.writeheader()
    w.writerows(products)

# ---------- orders.json (nested: items array, shipping struct, tags array) ----------
statuses = ["placed","shipped","delivered","cancelled","returned"]
orders = []
order_id = 1
for _ in range(4000):
    cust = random.choice(customers)
    n_items = random.randint(1, 5)
    items = []
    for _ in range(n_items):
        prod = random.choice(products)
        items.append({
            "product_id": prod["product_id"],
            "qty": random.randint(1, 4),
            "unit_price": prod["unit_price"],
        })
    order_date = datetime(2023,1,1) + timedelta(days=random.randint(0, 700),
                                                  hours=random.randint(0,23))
    orders.append({
        "order_id": f"O{order_id:06d}",
        "customer_id": cust["customer_id"],
        "order_ts": order_date.isoformat(),
        "status": random.choice(statuses),
        "items": items,
        "shipping": {
            "method": random.choice(["standard","express","pickup"]),
            "address": {
                "city": random.choice(["Oklahoma City","Dallas","Dhaka","Toronto","London", None]),
                "zip": f"{random.randint(10000,99999)}"
            }
        },
        "tags": random.sample(["gift","promo","bulk","first_order","loyalty"], k=random.randint(0,2)),
    })
    order_id += 1

with open(f"{OUT}/orders.json", "w") as f:
    for o in orders:
        f.write(json.dumps(o) + "\n")   # newline-delimited JSON, Spark-friendly

# ---------- reviews.json (nested, links to product + customer, some missing ratings) ----------
reviews = []
for i in range(1, 1201):
    reviews.append({
        "review_id": f"R{i:05d}",
        "product_id": random.choice(products)["product_id"],
        "customer_id": random.choice(customers)["customer_id"],
        "rating": random.choice([1,2,3,4,5,None]),
        "comment": random.choice(["Great!","Not as expected","Fast shipping","Broken on arrival",
                                    "Would buy again", ""]),
        "review_ts": (datetime(2023,1,1) + timedelta(days=random.randint(0,700))).isoformat(),
    })
with open(f"{OUT}/reviews.json", "w") as f:
    for r in reviews:
        f.write(json.dumps(r) + "\n")

print("Done. Files in ./data/: customers.csv, products.csv, orders.json, reviews.json")
