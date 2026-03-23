                          ##### Gold Layer - Joins & Aggregations #######
# Import Required Libraries
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

############################ JOINED ############################

@dp.table(
    name="gold_cust_acc_trns_mv",
    comment="Customer Account Transactions Materialized View"
)
def gold_cust_acc_trns_mv():
    customers = dp.read("silver_customers_transformed")
    tx = dp.read("silver_accounts_transactions_transformed")

    joined = customers.join(
        tx,
        on="customer_id",
        how="inner"
    )

    return joined


############################ AGGREGATIONS ############################

@dp.table(
    name="gold_cust_acc_trans_agg",
    comment="Gold layer aggregated MV of customer + account transactions"
)
def gold_cust_acc_trans_agg():
    df = dp.read("gold_cust_acc_trns_mv")  # source wide table with customer + txn rows

    return (
        df.groupBy(
            "customer_id", "name", "gender", "city", "status",
            "income_range", "risk_segment", "customer_age", "tenure_days"
        )
        .agg(
            countDistinct("account_id").alias("accounts_count"),
            count("*").alias("txn_count"),
            sum(
                when(col("txn_type") == "CREDIT", col("txn_amount"))
                .otherwise(lit(0.0))
            ).alias("total_credits"),
            sum(
                when(col("txn_type") == "DEBIT", col("txn_amount"))
                .otherwise(lit(0.0))
            ).alias("total_debits"),
            avg(col("txn_amount")).alias("avg_txn_amount"),
            min("txn_date").alias("first_txn_date"),
            max("txn_date").alias("last_txn_date"),
            countDistinct("txn_channel").alias("channels_used")
        )
    )