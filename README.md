# Automated Lakeflow Declarative Pipleines/Workflows --- Bank Project
 In this project, I have demonstrated the use Databrick platform to create Automated Lakeflow Declarative Pipeline(previous DLT), this project demonstrates how Databricks Workflows makes data engineering pipelines declarative, and scalable, while powering dashboards for real-time insights.

## Project Details

<img width="1906" height="928" alt="Screenshot 2026-03-23 154639" src="https://github.com/user-attachments/assets/e33c5338-95f8-4b49-9abf-3afb5607694b" />


- `Landing_Layer.py`: Defines both landing_customers_incremental and landing_accounts_incremental as streaming tables using Autoloader, with correct schemas. These are the inputs for your bronze layer.

- `bronze_layer.py`: Reads from landing sources, cleans/transforms, then writes bronze_customers_clean and bronze_accounts_clean. Expectation columns now match transformations.

- `silver_layer.py`: Reads bronze_customers_clean and bronze_accounts_clean directly using streaming patterns:
spark.readStream.table("bronze_customers_clean")
spark.readStream.table("bronze_accounts_clean") Then performs additional transformation and CDC flows. No incorrect references.

- `gold_layer.py`: Batch reads from silver layers using dp.read (deprecated, but works) for joins/aggregations
