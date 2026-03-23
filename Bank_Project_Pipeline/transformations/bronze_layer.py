##### Bronze Layer - Data Cleaning - Customers #####

# Import Required Libraries
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a streaming table
@dlt.table(
    name="bronze_customers_clean",
    comment="cleaned data from the customers ingested layer data"
)
# @dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
# @dlt.expect_or_drop("valid_customer_name", "name IS NOT NULL")
# @dlt.expect_or_drop("valid_dob", "dob IS NOT NULL")
# @dlt.expect_or_drop("valid_city", "city IS NOT NULL")
# @dlt.expect_or_drop("valid_join_date", "join_date IS NOT NULL")
# @dlt.expect_or_drop(
#     "valid_email",
#     "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
# )
# @dlt.expect_or_drop("valid_phone", "phone_number IS NOT NULL")
# @dlt.expect_or_drop("valid_channel", "preferred_channel IS NOT NULL")
# @dlt.expect_or_drop("valid_occupation", "occupation IS NOT NULL")
# @dlt.expect_or_drop("valid_income", "income_range IS NOT NULL")
# @dlt.expect_or_drop("valid_risk_segment", "risk_segment IS NOT NULL")
# @dlt.expect_or_drop("valid_address", "address IS NOT NULL")
# @dlt.expect("valid_gender", "gender IS NOT NULL")
# @dlt.expect("valid_status", "status IS NOT NULL")
def bronze_customers_clean():
    df = spark.readStream.table("landing_customers_incremental")

    # Data transformations
    df = df.withColumn("name", upper(col("name")))
    df = df.withColumn("email", lower(col("email")))
    df = df.withColumn("occupation", upper(col("occupation")))
    df = df.withColumn("city", upper(col("city")))
    df = df.withColumn("income_range", upper(col("income_range")))
    df = df.withColumn("risk_segment", upper(col("risk_segment")))
    df = df.withColumn("preferred_channel", upper(col("preferred_channel")))
    df = df.withColumn(
        "gender",
        when(col("gender") == "M", lit("MALE"))
        .when(col("gender") == "F", lit("FEMALE"))
        .otherwise("Unknown")
    )
    df = df.withColumn(
        "status",
        upper(
            when(
                col("status").isNull() | (trim(col("status")) == ""),
                lit("UNKNOWN")
            ).otherwise(col("status"))
        )
    )
    df = df.withColumn("phone_number", trim(col("phone_number")))
    df = df.withColumn("phone_number", regexp_replace(col("phone_number"), r"[^0-9\+]+", ""))
    df = df.filter(col("phone_number").rlike(r"^\+44\d{10}$"))
    df = df.filter(col("preferred_channel").isin("MOBILE", "ONLINE", "BRANCH", "ATM"))
    df = df.filter(col("income_range").isin("LOW", "MEDIUM", "HIGH", "VERY HIGH"))
    df = df.filter(col("risk_segment").isin("LOW", "MEDIUM", "HIGH"))

    return df


##### Bronze Layer - Data Cleaning - Accounts #####

@dlt.table(
    name="bronze_accounts_clean",
    comment="This table contains the cleaned data from the transactions ingestion layer")
@dlt.expect_or_fail("valid_account_id", "account_id IS NOT NULL")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("account_type", "account_type IS NOT NULL")
@dlt.expect_or_drop("valid_balance", "balance IS NOT NULL")
@dlt.expect_or_drop("valid_txn_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_txn_amount", "txn_amount IS NOT NULL")
@dlt.expect_or_drop("valid_txn_type", "txn_type IS NOT NULL")
@dlt.expect_or_drop("valid_txn_channel", "txn_channel IS NOT NULL")
def bronze_accounts_clean():
    df = spark.readStream.table("landing_accounts_incremental")

    # Data transformations
    df = df.withColumn("account_type", upper(col("account_type")))
    df = df.withColumn("txn_channel", upper(col("txn_channel")))
    df = df.withColumn("txn_type", upper(col("txn_type")))
    df = df.withColumn(
        "txn_type",
        when(col("txn_type") == "DEBITT", "DEBIT")
        .when(col("txn_type") == "CREDIT", "CREDIT")
        .otherwise("UNKNOWN")
    )

    return df
