# PySparkLearning

Learn PySpark from zero, using Databricks Community Edition purely as free compute.
Dataset: `GlobalCart` — customers, products, orders (nested JSON), reviews (nested JSON).
Deliberately messy: nulls, duplicates, inconsistent date formats, nested structs/arrays.

## 1. One-time: upload data to S3

1. AWS Console → S3 → **Create bucket** → name it e.g. `yourname-pysparklearning` → Create.
2. Open the bucket → **Upload** → **Add files** → select the 4 files from `data/`
   (`customers.csv`, `products.csv`, `orders.json`, `reviews.json`) → Upload.
3. IAM → Users → your user → **Security credentials** → **Create access key**
   → choose "Local code" → copy the **Access Key ID** and **Secret Access Key**
   (you'll paste these into Databricks, once, in Task 2).

## 2. One-time: Databricks Community Edition setup

1. Go to databricks.com/try-databricks → sign up for **Community Edition** (free, no card).
2. Log in → left sidebar → **Compute** → **Create compute** → leave defaults →
   **Create compute**. Wait until it shows a green dot (~3-5 min).
3. Left sidebar → **Workspace** → your user folder → **Create** → **Notebook**.
4. Name it `Task01_Setup`. Language: **Python**. Cluster: pick the one you just created.
5. That's it — every task below is just: paste the code cell → click ▶ (Run cell).

You never need to touch Databricks' own docs — the tasks tell you exactly what to click.
