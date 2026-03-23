##### Customers 2023 file ingestion
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("preferred_channel", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("income_range", StringType(), True),
    StructField("risk_segment", StringType(), True)
])
# create a streaming table
@dlt.table(
    name="landing_customers_incremental",
    comment="landing customers data"
)
# Use Autoloader
def landing_customers_incremental():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/bank_project/bank_project/bank_project_volume/customers/")
    )

          ##### Accounts 2023 file ingestion

# create the accounts schema

accounts_schema = StructType([
    StructField("account_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("txn_id", LongType(), True),
    StructField("txn_date", DateType(), True),
     StructField("txn_type", StringType(), True),
    StructField("txn_amount", DoubleType(), True),
    StructField("txn_channel", StringType(), True)
])
# create a streaming table      

@dlt.table(
    name="landing_accounts_incremental",
    comment="landing accounts data"
)
# Use Autoloader

def landing_accounts_incremental():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .schema(accounts_schema)
        .load("/Volumes/bank_project/bank_project/bank_project_volume/accounts/")
    )
