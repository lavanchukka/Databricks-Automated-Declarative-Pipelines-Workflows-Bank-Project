import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

################ DATA TRANSFORMATION - CUSTOMERS TABLE ################

@dlt.table(
    name="silver_customers_transformed",
    comment="Transformed customers table")

def silver_customers_transformed():
    df = spark.readStream.table("bronze_customers_clean")
    df = df.withColumn("customer_age", when(col("dob").isNotNull(), floor(months_between(current_date(), col("dob")) / 12)
        ).otherwise(lit(None))
    )
    df = df.withColumn(
        "tenure_days",
        when(
            col("join_date").isNotNull(),
            datediff(current_date(), col("join_date"))
        ).otherwise(lit(None))
    )
    df = df.withColumn(
        "dob_out_of_range_flag",
        (col("dob") < lit("1900-01-01")) | (col("dob") > current_date())
    )
    df = df.withColumn("transformation_date", current_timestamp())

    return df


#---------------- SCD1 - APPLY_CHANGES 

dlt.create_streaming_table("silver_customers_transformed_scd1")

dlt.apply_changes(
    target="silver_customers_transformed_scd1",
    source="silver_customers_transformed",
    keys=["customer_id"],
    sequence_by=col("transformation_date"),
    stored_as_scd_type=1,
    except_column_list=["transformation_date"]
)

#--- view

@dlt.view(
    name="silver_customers_transformed_view",
    comment="View of silver_customers_transformed table"
)
def silver_customers_transformed_view():
   return spark.readStream.table("silver_customers_transformed")


################## DATA TRANSFORMATION - TRANSACTIONS TABLE 
@dlt.table(
    name="silver_accounts_transactions_transformed",
    comment="Transformed accounts_transactions table")
def silver_accounts_transactions_transformed():
    df = spark.readStream.table("bronze_accounts_clean")
    df = df.withColumn(
        "channel_type",
        when(
            (col("txn_channel") == "ATM") | (col("txn_channel") == "BRANCH"),
            lit("PHYSICAL")
        ).otherwise(lit("DIGITAL"))
    )
    df = df.withColumn("txn_year", year(col("txn_date"))) \
           .withColumn("txn_month", month(col("txn_date"))) \
           .withColumn("txn_day", dayofmonth(col("txn_date")))
    df = df.withColumn(
        "txn_direction",
        when(col("txn_type") == "DEBIT", lit("OUT")).otherwise(lit("IN"))
    )
    df = df.withColumn("acc_transformation_date", current_timestamp())

    return df


#---------------- SCD2 - AUTO CDC---------------------#

dlt.create_streaming_table("silver_accounts_transactions_transformed_scd2")

dlt.create_auto_cdc_flow(
    target="silver_accounts_transactions_transformed_scd2",
    source="silver_accounts_transactions_transformed",
    keys=["txn_id"],
    sequence_by=col("acc_transformation_date"),
    except_column_list=["acc_transformation_date"],
    stored_as_scd_type=2
)


#--- Materialized view

@dlt.table(
    name="silver_accounts_transactions_transformed_view",
    comment="View of silver_accounts_transformed table"
)
def silver_accounts_transactions_transformed_view():
    return spark.read.table("silver_accounts_transactions_transformed")

