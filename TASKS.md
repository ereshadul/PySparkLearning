# Phase 1 — PySpark, Basic → Advanced (22 tasks)

Each task = one notebook cell (or a couple). Run it, read the output, move on.
"PySpark goal" tells you WHY the task matters — that's the part to actually learn.
Databricks steps are just "click this."

Do Task 2 once (it stores your S3 keys for the session) — every later task starts
by reading straight from S3 with the same 3 lines.

---

### Task 1 — Your first DataFrame
**PySpark goal:** see that a DataFrame is just a table Spark can compute on in parallel.
```python
data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()
```
**Click:** new cell in `Task01_Setup` → paste → Run cell (▶ or Shift+Enter).

---

### Task 2 — Connect to S3 and read customers.csv
**PySpark goal:** Spark reads data from cloud storage the same way it reads local
files — only the path changes.
```python
ACCESS_KEY = "PASTE_YOUR_ACCESS_KEY"
SECRET_KEY = "PASTE_YOUR_SECRET_KEY"
BUCKET = "yourname-pysparklearning"

spark.conf.set("fs.s3a.access.key", ACCESS_KEY)
spark.conf.set("fs.s3a.secret.key", SECRET_KEY)

customers = spark.read.csv(f"s3a://{BUCKET}/customers.csv", header=True, inferSchema=True)
customers.show(5)
```

---

### Task 3 — Explore the schema
**PySpark goal:** always check schema before writing logic — types drive everything.
```python
customers.printSchema()
customers.describe().show()
print(customers.count())
```

---

### Task 4 — Select, rename, add columns
**PySpark goal:** `select`, `withColumnRenamed`, `withColumn` are 90% of daily PySpark.
```python
from pyspark.sql.functions import col, upper

customers2 = (customers
    .select("customer_id", "first_name", "email", "country")
    .withColumnRenamed("first_name", "fname")
    .withColumn("fname_upper", upper(col("fname"))))
customers2.show(5)
```

---

### Task 5 — Filter and handle nulls
**PySpark goal:** `filter`/`where`, and the difference between dropping vs filling nulls.
```python
customers.filter(col("country").isNull()).count()
customers_clean = customers.na.fill({"loyalty_tier": "bronze"}).na.drop(subset=["email"])
customers_clean.show(5)
```

---

### Task 6 — Fix the messy date column
**PySpark goal:** `signup_date` has 3 different formats. Real data does this constantly.
```python
from pyspark.sql.functions import coalesce, to_date

fixed = customers.withColumn(
    "signup_date_clean",
    coalesce(
        to_date("signup_date", "yyyy-MM-dd"),
        to_date("signup_date", "MM/dd/yyyy"),
        to_date("signup_date", "dd-MM-yyyy"),
    )
)
fixed.select("signup_date", "signup_date_clean").show(10)
```

---

### Task 7 — Deduplicate customers
**PySpark goal:** `dropDuplicates` vs deciding WHICH duplicate to keep (window trick preview).
```python
print("before:", customers.count())
deduped = customers.dropDuplicates(["customer_id"])
print("after:", deduped.count())
```

---

### Task 8 — Read the nested orders.json
**PySpark goal:** Spark infers nested schemas (structs, arrays) automatically from JSON.
```python
orders = spark.read.json(f"s3a://{BUCKET}/orders.json")
orders.printSchema()
orders.show(3, truncate=False)
```

---

### Task 9 — Explode the items array
**PySpark goal:** `explode` turns 1 row with an array into N rows — the #1 nested-data move.
```python
from pyspark.sql.functions import explode

order_items = orders.select("order_id", "customer_id", explode("items").alias("item"))
order_items.show(5, truncate=False)
```

---

### Task 10 — Flatten nested structs
**PySpark goal:** dot notation pulls fields out of structs (`items`, `shipping`).
```python
flat = order_items.select(
    "order_id", "customer_id",
    col("item.product_id").alias("product_id"),
    col("item.qty").alias("qty"),
    col("item.unit_price").alias("unit_price"),
)
flat.show(5)
```

---

### Task 11 — Join orders with customers
**PySpark goal:** joins — the most common source of bugs (dup keys, wrong join type).
```python
enriched = flat.join(deduped, "customer_id", "left")
enriched.select("order_id", "product_id", "email", "country").show(5)
```

---

### Task 12 — Join with products, compute line revenue
**PySpark goal:** chaining joins + a derived column.
```python
products = spark.read.csv(f"s3a://{BUCKET}/products.csv", header=True, inferSchema=True)

full = (flat.join(products, "product_id", "left")
    .withColumn("line_revenue", col("qty") * col("item.unit_price") if False else col("qty") * col("unit_price")))
full.select("order_id", "product_id", "qty", "unit_price", "line_revenue").show(5)
```
*(If that ternary line errors, just use `col("qty") * col("unit_price")` — it's a deliberate
gotcha: PySpark columns aren't Python values, so Python `if/else` on them doesn't work.)*

---

### Task 13 — groupBy + aggregations
**PySpark goal:** `groupBy().agg()` — revenue by category.
```python
from pyspark.sql.functions import sum as spark_sum, avg, count

by_cat = full.groupBy("category").agg(
    spark_sum("line_revenue").alias("total_revenue"),
    avg("line_revenue").alias("avg_line_revenue"),
    count("*").alias("n_lines"),
).orderBy(col("total_revenue").desc())
by_cat.show()
```

---

### Task 14 — Window functions: rank customers by spend
**PySpark goal:** windows compute per-group without collapsing rows (unlike groupBy).
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, sum as spark_sum

cust_spend = full.groupBy("customer_id").agg(spark_sum("line_revenue").alias("total_spend"))
w = Window.orderBy(col("total_spend").desc())
cust_spend.withColumn("spend_rank", rank().over(w)).show(10)
```

---

### Task 15 — Window functions: running total per customer over time
**PySpark goal:** partitioned + ordered windows — very common interview ask.
```python
orders_dated = orders.withColumn("order_date", to_date("order_ts"))
w2 = Window.partitionBy("customer_id").orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
orders_dated.withColumn("orders_so_far", count("order_id").over(w2)) \
    .select("customer_id", "order_date", "orders_so_far").show(10)
```

---

### Task 16 — Pivot: monthly revenue by category
**PySpark goal:** `pivot` reshapes long data to wide — common for reporting.
```python
from pyspark.sql.functions import month, year

full_dated = full.join(orders.select("order_id", "order_ts"), "order_id") \
    .withColumn("month", month("order_ts"))
full_dated.groupBy("category").pivot("month").agg(spark_sum("line_revenue")).show()
```

---

### Task 17 — UDF, and why to avoid it
**PySpark goal:** UDFs work but skip Spark's optimizer — use built-ins first, UDF as last resort.
```python
from pyspark.sql.functions import udf, when
from pyspark.sql.types import StringType

# built-in way (fast):
by_cat_tier = full.withColumn("tier",
    when(col("line_revenue") > 200, "high")
    .when(col("line_revenue") > 50, "mid")
    .otherwise("low"))

# UDF way (slower, shown for comparison):
def tier_udf(rev):
    if rev is None: return "unknown"
    return "high" if rev > 200 else "mid" if rev > 50 else "low"
tier_spark_udf = udf(tier_udf, StringType())
by_cat_tier2 = full.withColumn("tier", tier_spark_udf(col("line_revenue")))
by_cat_tier.select("order_id", "line_revenue", "tier").show(5)
```

---

### Task 18 — Read query plans, repartition
**PySpark goal:** `explain()` shows what Spark will actually do; partitions control parallelism.
```python
full.groupBy("category").agg(spark_sum("line_revenue")).explain()
print("partitions before:", full.rdd.getNumPartitions())
full_rep = full.repartition(8, "category")
print("partitions after:", full_rep.rdd.getNumPartitions())
```

---

### Task 19 — Cache and compare timing
**PySpark goal:** `.cache()` avoids recomputing a DataFrame used multiple times.
```python
import time
big = full.groupBy("customer_id").agg(spark_sum("line_revenue").alias("s"))

start = time.time()
big.count()
print("uncached:", time.time() - start)

big.cache()
big.count()  # materializes cache
start = time.time()
big.count()
print("cached:", time.time() - start)
```

---

### Task 20 — Write results back to S3 as partitioned Parquet
**PySpark goal:** Parquet + partitioning is the standard way pipelines hand off data.
```python
full_dated.write.mode("overwrite").partitionBy("month").parquet(
    f"s3a://{BUCKET}/output/revenue_by_month/"
)
```

---

### Task 21 — Read it back, confirm partition pruning
**PySpark goal:** reading a filtered partition should skip the other files entirely.
```python
back = spark.read.parquet(f"s3a://{BUCKET}/output/revenue_by_month/")
back.filter(col("month") == 6).explain()  # look for "PartitionFilters" in the plan
```

---

### Task 22 — End-to-end mini pipeline
**PySpark goal:** stitch everything into one script — this is what a real job looks like.
```python
def run_pipeline(spark, bucket):
    customers = spark.read.csv(f"s3a://{bucket}/customers.csv", header=True, inferSchema=True).dropDuplicates(["customer_id"])
    products = spark.read.csv(f"s3a://{bucket}/products.csv", header=True, inferSchema=True)
    orders = spark.read.json(f"s3a://{bucket}/orders.json")

    items = orders.select("order_id", "customer_id", explode("items").alias("item"))
    flat = items.select("order_id", "customer_id", col("item.product_id").alias("product_id"),
                         col("item.qty").alias("qty"), col("item.unit_price").alias("unit_price"))
    full = flat.join(products, "product_id").withColumn("line_revenue", col("qty") * col("unit_price"))

    return full.groupBy("category").agg(spark_sum("line_revenue").alias("total_revenue")) \
               .orderBy(col("total_revenue").desc())

run_pipeline(spark, BUCKET).show()
```

Done Phase 1 → you can read/write nested + tabular data, join, aggregate, window,
optimize, and productionize a small pipeline. That's the real PySpark job floor.
