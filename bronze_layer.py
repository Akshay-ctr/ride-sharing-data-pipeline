# Ride-Sharing Incremental Data Pipeline
# Layer: Bronze
# Description: Ingest raw data into Delta tables
# Author: Akshay

from pyspark.sql import functions as F

# Create Schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")


# Riders Ingestion
riders_df = spark.read.csv(
    "dbfs:/FileStore/riders_full.csv",
    header=True,
    inferSchema=True
).withColumn("Load_Date", F.current_date()) \
 .withColumn("File_Name", F.lit("riders_full.csv"))

riders_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.bronze.riders_raw")

print("Riders bronze loaded")


# Drivers Ingestion
drivers_df = spark.read.csv(
    "dbfs:/FileStore/drivers_full.csv",
    header=True,
    inferSchema=True
).withColumn("Load_Date", F.current_date()) \
 .withColumn("File_Name", F.lit("drivers_full.csv"))

drivers_df.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.bronze.drivers_raw")

print("Drivers bronze loaded")


# Trips Ingestion (Incremental Files)
for day in range(1, 8):
    file_name = f"trips_day_{day}.csv"

    df = spark.read.csv(
        f"dbfs:/FileStore/{file_name}",
        header=True,
        inferSchema=True
    ).withColumn("Load_Date", F.current_date()) \
     .withColumn("File_Name", F.lit(file_name))

    df.write.format("delta").mode("append") \
        .saveAsTable("workspace.bronze.trips_raw")

    print(f"{file_name} loaded")

print("Bronze layer completed successfully")