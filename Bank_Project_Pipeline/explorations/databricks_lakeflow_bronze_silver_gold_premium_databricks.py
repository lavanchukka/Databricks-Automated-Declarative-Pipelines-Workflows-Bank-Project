# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Lakeflow Pipeline Notebook  
# MAGIC ## Bronze → Silver → Gold
# MAGIC
# MAGIC This notebook is a polished, Databricks-friendly version of the code corresponding to the tutorial segments you requested:
# MAGIC
# MAGIC - **Bronze Work** — `2:07:15`
# MAGIC - **Silver Work** — `3:03:59`
# MAGIC - **Gold Work** — `3:37:32`
# MAGIC
# MAGIC It is organized like a project handoff notebook rather than a raw transcript, so each stage is easier to read, review, and import into Databricks.
# MAGIC
# MAGIC > Note: this notebook is based on the earlier reconstructed extraction, aligned to a matching companion implementation of the tutorial flow where video frames were not fully readable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook covers
# MAGIC
# MAGIC This notebook builds a simple multi-layer Lakeflow / DLT-style pipeline:
# MAGIC
# MAGIC 1. **Bronze** ingests raw customer, product, and sales data.
# MAGIC 2. **Silver** applies type cleanup, enrichment, and CDC logic.
# MAGIC 3. **Gold** creates dimensions, facts, and a business aggregate.
# MAGIC
# MAGIC The code is grouped by logical pipeline module so you can either:
# MAGIC - keep it as one notebook for study and review, or
# MAGIC - split the cells back into separate source files for a production repo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended project layout
# MAGIC
# MAGIC ```text
# MAGIC transformations/
# MAGIC ├── bronze/
# MAGIC │   ├── ingestion_customers.py
# MAGIC │   ├── ingestion_products.py
# MAGIC │   └── ingestion_sales.py
# MAGIC ├── silver/
# MAGIC │   ├── transform_customers.py
# MAGIC │   ├── transform_products.py
# MAGIC │   └── transform_sales.py
# MAGIC └── gold/
# MAGIC     ├── dim_customers.py
# MAGIC     ├── dim_products.py
# MAGIC     ├── fact_sales.py
# MAGIC     └── business_sales.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution flow
# MAGIC
# MAGIC ```text
# MAGIC Raw source tables
# MAGIC    │
# MAGIC    ├── dlt.source.customers ───────────────┐
# MAGIC    ├── dlt.source.products ────────────────┼─> Bronze staging tables
# MAGIC    ├── dlt.source.sales_east ────────┐     │
# MAGIC    └── dlt.source.sales_west ────────┴─────┘
# MAGIC
# MAGIC Bronze:
# MAGIC - customers_stg
# MAGIC - products_stg
# MAGIC - sales_stg
# MAGIC
# MAGIC Silver:
# MAGIC - customers_enr_view → customers_enr
# MAGIC - products_enr_view  → products_enr
# MAGIC - sales_enr_view     → sales_enr
# MAGIC
# MAGIC Gold:
# MAGIC - dim_customers
# MAGIC - dim_products
# MAGIC - fact_sales
# MAGIC - business_sales
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assumptions before you run this
# MAGIC
# MAGIC This notebook assumes:
# MAGIC
# MAGIC - `dlt` / Lakeflow Declarative Pipelines APIs are available in the target environment.
# MAGIC - The source tables already exist:
# MAGIC   - `dlt.source.customers`
# MAGIC   - `dlt.source.products`
# MAGIC   - `dlt.source.sales_east`
# MAGIC   - `dlt.source.sales_west`
# MAGIC - The source schemas include fields used by the transformations, such as:
# MAGIC   - `customer_id`, `customer_name`, `last_updated`
# MAGIC   - `product_id`, `price`, `last_updated`
# MAGIC   - `sales_id`, `customer_id`, `product_id`, `quantity`, `amount`, `sale_timestamp`
# MAGIC - CDC sequencing columns are trustworthy and monotonic enough for merge/update behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline object map
# MAGIC
# MAGIC | Layer | Object | Type | Role |
# MAGIC |---|---|---|---|
# MAGIC | Bronze | `customers_stg` | streaming table | raw customer ingestion with expectations |
# MAGIC | Bronze | `products_stg` | streaming table | raw product ingestion with expectations |
# MAGIC | Bronze | `sales_stg` | streaming table | unified sales stream from east + west |
# MAGIC | Silver | `customers_enr_view` | view | customer cleanup and normalization |
# MAGIC | Silver | `customers_enr` | streaming table | type-1 CDC customer table |
# MAGIC | Silver | `products_enr_view` | view | product casting / cleanup |
# MAGIC | Silver | `products_enr` | streaming table | type-1 CDC product table |
# MAGIC | Silver | `sales_enr_view` | view | sales enrichment with `total_amount` |
# MAGIC | Silver | `sales_enr` | streaming table | type-1 CDC sales table |
# MAGIC | Gold | `dim_customers` | streaming table | type-2 customer dimension |
# MAGIC | Gold | `dim_products` | streaming table | type-2 product dimension |
# MAGIC | Gold | `fact_sales` | streaming table | fact-style sales table |
# MAGIC | Gold | `business_sales` | table | aggregated business reporting output |

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Work — `2:07:15`
# MAGIC
# MAGIC The Bronze layer lands raw data with basic quality checks.  
# MAGIC This is the thinnest layer in the pipeline: preserve data as much as possible, but reject obviously invalid records.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze module: `ingestion_customers.py`
# MAGIC
# MAGIC **Input:** `dlt.source.customers`  
# MAGIC **Output:** `customers_stg`  
# MAGIC **Purpose:** Validate key customer fields and stream raw customer records into the Bronze layer.

# COMMAND ----------

# bronze/ingestion_customers.py
# Purpose: Ingest raw customers into Bronze with basic expectations.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Customer expectations
customer_rules = {
    "rule_1": "customer_id IS NOT NULL",
    "rule_2": "customer_name IS NOT NULL",
}

# Ingest customers
@dlt.table(name="customers_stg")
@dlt.expect_all(customer_rules)
def customers_stg():
    df = spark.readStream.table("dlt.source.customers")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze module: `ingestion_products.py`
# MAGIC
# MAGIC **Input:** `dlt.source.products`  
# MAGIC **Output:** `products_stg`  
# MAGIC **Purpose:** Validate essential product fields and stream raw product records into the Bronze layer.

# COMMAND ----------

# bronze/ingestion_products.py
# Purpose: Ingest raw products into Bronze with basic expectations.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Product expectations
product_rules = {
    "rule_1": "product_id IS NOT NULL",
    "rule_2": "price >= 0",
}

# Ingest products
@dlt.table(name="products_stg")
@dlt.expect_all(product_rules)
def products_stg():
    df = spark.readStream.table("dlt.source.products")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze module: `ingestion_sales.py`
# MAGIC
# MAGIC **Inputs:** `dlt.source.sales_east`, `dlt.source.sales_west`  
# MAGIC **Output:** `sales_stg`  
# MAGIC **Purpose:** Create a unified Bronze sales stream by appending multiple regional feeds into one target table.

# COMMAND ----------

# bronze/ingestion_sales.py
# Purpose: Create a unified Bronze sales stream from east and west sources.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Sales expectations
sales_rules = {
    "rule_1": "sales_id IS NOT NULL",
}

# Create empty streaming table
dlt.create_streaming_table(
    name="sales_stg",
    expect_all_or_drop=sales_rules,
)

# Append east sales into sales_stg
@dlt.append_flow(target="sales_stg")
def east_sales():
    df = spark.readStream.table("dlt.source.sales_east")
    return df

# Append west sales into sales_stg
@dlt.append_flow(target="sales_stg")
def west_sales():
    df = spark.readStream.table("dlt.source.sales_west")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze smoke-test queries
# MAGIC
# MAGIC Use these after the Bronze layer is materialized to confirm data is landing as expected.

# COMMAND ----------

# Optional validation / smoke checks for Bronze tables
for table_name in ["customers_stg", "products_stg", "sales_stg"]:
    print(f"\n===== {table_name} =====")
    spark.read.table(table_name).limit(10).show(truncate=False)
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Work — `3:03:59`
# MAGIC
# MAGIC The Silver layer standardizes and enriches Bronze data.  
# MAGIC This is where cleanup, computed fields, and CDC application logic are introduced.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver module: `transform_customers.py`
# MAGIC
# MAGIC **Input:** `customers_stg`  
# MAGIC **Outputs:** `customers_enr_view`, `customers_enr`  
# MAGIC **Purpose:** Normalize customer text and apply type-1 CDC into the enriched customer table.

# COMMAND ----------

# silver/transform_customers.py
# Purpose: Normalize customer records and load them into a Silver CDC target.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Transform customers
@dlt.view(name="customers_enr_view")
def customers_stg_trns():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name", upper(col("customer_name")))
    return df

# Create destination silver table
dlt.create_streaming_table(name="customers_enr")

dlt.create_auto_cdc_flow(
    target="customers_enr",
    source="customers_enr_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver module: `transform_products.py`
# MAGIC
# MAGIC **Input:** `products_stg`  
# MAGIC **Outputs:** `products_enr_view`, `products_enr`  
# MAGIC **Purpose:** Standardize product types and apply type-1 CDC into the enriched product table.

# COMMAND ----------

# silver/transform_products.py
# Purpose: Cast product fields and load them into a Silver CDC target.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Transform products
@dlt.view(name="products_enr_view")
def products_stg_trns():
    df = spark.readStream.table("products_stg")
    df = df.withColumn("price", col("price").cast(IntegerType()))
    return df

# Create destination silver table
dlt.create_streaming_table(name="products_enr")

dlt.create_auto_cdc_flow(
    target="products_enr",
    source="products_enr_view",
    keys=["product_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver module: `transform_sales.py`
# MAGIC
# MAGIC **Input:** `sales_stg`  
# MAGIC **Outputs:** `sales_enr_view`, `sales_enr`  
# MAGIC **Purpose:** Enrich sales with calculated metrics and apply type-1 CDC into the Silver sales table.

# COMMAND ----------

# silver/transform_sales.py
# Purpose: Derive total_amount and load sales into a Silver CDC target.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt
from pyspark.sql.functions import *

# Transform sales
@dlt.view(name="sales_enr_view")
def sales_stg_trns():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount", col("quantity") * col("amount"))
    return df

# Create destination silver table
dlt.create_streaming_table(name="sales_enr")

dlt.create_auto_cdc_flow(
    target="sales_enr",
    source="sales_enr_view",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver validation checks
# MAGIC
# MAGIC These checks help confirm the enrichment logic is producing expected columns and row counts.

# COMMAND ----------

# Optional validation / smoke checks for Silver tables
for table_name in ["customers_enr", "products_enr", "sales_enr"]:
    print(f"\n===== {table_name} =====")
    spark.read.table(table_name).limit(10).show(truncate=False)
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}").show()

# Targeted check for the calculated sales metric
spark.sql("""
SELECT sales_id, quantity, amount, total_amount
FROM sales_enr
LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Work — `3:37:32`
# MAGIC
# MAGIC The Gold layer publishes analytics-ready outputs:
# MAGIC - dimensions for customers and products,
# MAGIC - a fact-like sales table,
# MAGIC - and a business aggregate for reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold module: `dim_customers.py`
# MAGIC
# MAGIC **Input:** `customers_enr_view`  
# MAGIC **Output:** `dim_customers`  
# MAGIC **Purpose:** Build a customer dimension using type-2 history tracking.

# COMMAND ----------

# gold/dim_customers.py
# Purpose: Create a type-2 customer dimension from the Silver customer view.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Create empty streaming table
dlt.create_streaming_table(name="dim_customers")

# Auto CDC flow
dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_enr_view",
    keys=["customer_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold module: `dim_products.py`
# MAGIC
# MAGIC **Input:** `products_enr_view`  
# MAGIC **Output:** `dim_products`  
# MAGIC **Purpose:** Build a product dimension using type-2 history tracking.

# COMMAND ----------

# gold/dim_products.py
# Purpose: Create a type-2 product dimension from the Silver product view.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Create empty streaming table
dlt.create_streaming_table(name="dim_products")

# Auto CDC flow
dlt.create_auto_cdc_flow(
    target="dim_products",
    source="products_enr_view",
    keys=["product_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold module: `fact_sales.py`
# MAGIC
# MAGIC **Input:** `sales_enr_view`  
# MAGIC **Output:** `fact_sales`  
# MAGIC **Purpose:** Publish a fact-style sales table using type-1 CDC behavior.

# COMMAND ----------

# gold/fact_sales.py
# Purpose: Create the fact-style sales table from the Silver sales view.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt

# Create empty streaming table
dlt.create_streaming_table(name="fact_sales")

# Auto CDC flow
dlt.create_auto_cdc_flow(
    target="fact_sales",
    source="sales_enr_view",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None,
    name=None,
    once=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold module: `business_sales.py`
# MAGIC
# MAGIC **Inputs:** `fact_sales`, `dim_customers`, `dim_products`  
# MAGIC **Output:** `business_sales`  
# MAGIC **Purpose:** Produce an aggregated business-facing table grouped by `region` and `category`.

# COMMAND ----------

# gold/business_sales.py
# Purpose: Join fact and dimensions to publish aggregated business metrics.
# Source section: extracted/reconstructed from the tutorial-aligned implementation.

import dlt
from pyspark.sql.functions import sum

# Create materialized business view
@dlt.table(name="business_sales")
def business_sales():
    df_fact = spark.read.table("fact_sales")
    df_dimCust = spark.read.table("dim_customers")
    df_dimProd = spark.read.table("dim_products")

    df_join = (
        df_fact.join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id, "inner")
               .join(df_dimProd, df_fact.product_id == df_dimProd.product_id, "inner")
    )

    df_prun = df_join.select("region", "category", "total_amount")
    df_agg = df_prun.groupBy("region", "category").agg(sum("total_amount").alias("total_sales"))
    return df_agg

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold validation checks
# MAGIC
# MAGIC These are useful for verifying final outputs once the Gold layer is available.

# COMMAND ----------

# Optional validation / smoke checks for Gold outputs
for table_name in ["dim_customers", "dim_products", "fact_sales", "business_sales"]:
    print(f"\n===== {table_name} =====")
    spark.read.table(table_name).limit(10).show(truncate=False)
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}").show()

# Business-facing aggregate check
spark.sql("""
SELECT region, category, total_sales
FROM business_sales
ORDER BY total_sales DESC
LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common failure points
# MAGIC
# MAGIC Watch for these when moving the notebook into a real workspace:
# MAGIC
# MAGIC 1. **Missing source tables**  
# MAGIC    The pipeline depends on `dlt.source.*` objects already existing.
# MAGIC
# MAGIC 2. **CDC sequencing issues**  
# MAGIC    `last_updated` and `sale_timestamp` must be valid for deterministic merge behavior.
# MAGIC
# MAGIC 3. **Schema mismatches**  
# MAGIC    If `price`, `amount`, or `quantity` arrive as unexpected types, downstream computations may fail.
# MAGIC
# MAGIC 4. **Dimension join duplication**  
# MAGIC    In real type-2 models, joining facts directly to dimensions on business keys may need careful effective-date handling.
# MAGIC
# MAGIC 5. **Environment mismatch**  
# MAGIC    Some workspaces use slightly different naming or runtime expectations for DLT / Lakeflow APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production hardening ideas
# MAGIC
# MAGIC A stronger production version of this notebook would usually add:
# MAGIC
# MAGIC - explicit schema declarations,
# MAGIC - quarantine/error tables,
# MAGIC - data quality dashboards,
# MAGIC - audit columns,
# MAGIC - idempotency tests,
# MAGIC - watermarking and late-arrival handling,
# MAGIC - surrogate keys for dimensions,
# MAGIC - effective-date joins between facts and SCD2 dimensions,
# MAGIC - unit tests for transformation logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final notes
# MAGIC
# MAGIC This notebook is designed to be a **clean study / handoff version** of the Bronze, Silver, and Gold sections you requested.  
# MAGIC You can now use it in three ways:
# MAGIC
# MAGIC - as a readable walkthrough notebook,
# MAGIC - as a base for a Databricks import,
# MAGIC - or as a template for splitting into repo-based source files again.
