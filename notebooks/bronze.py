# Define your cloud container path using the ABFSS protocol (Azure Blob File System)
# Replace <your-account-name> with the exact storage name we defined in Terraform
storage_account = "sustdata9512"
container_name  = "raw-landing"

# Construct the full cloud path string
source_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"

print(f"Targeting source path: {source_path}")

from pyspark.sql import functions as F

# Source path
source_path = "abfss://raw-landing@sustdata9512.dfs.core.windows.net/"

# Unity Catalog
catalog = "dbw_sustainability_platform"
schema = "default"
table = f"{catalog}.{schema}.bronze_esg_news"

# Auto Loader metadata stored in a Unity Catalog Volume
schema_location = (
    "/Volumes/dbw_sustainability_platform/default/esg_volume/"
    "schemas/esg_raw_ingestion"
)

checkpoint_location = (
    "/Volumes/dbw_sustainability_platform/default/esg_volume/"
    "checkpoints/esg_raw_ingestion"
)

# Read JSON files from ADLS Gen2
df_bronze_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaLocation", schema_location)
         .load(source_path)
)

# Create the destination table if it doesn't already exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table}
USING DELTA
""")

# Write stream to Delta
query_bronze = (
    df_bronze_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(table)
)

query_bronze.awaitTermination()