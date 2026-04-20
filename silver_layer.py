# Ride-Sharing Incremental Data Pipeline
# Layer: Silver
# Description: Data cleaning + incremental processing + upsert logic
# Author: Akshay

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -----------------------------
# Riders Cleaning
# -----------------------------
riders_silver = spark.table("workspace.bronze.riders_raw") \
    .dropDuplicates(["Rider_ID"]) \
    .withColumn("Rating",
        F.when(F.col("Rating").isNull(), F.lit(0.0)).otherwise(F.col("Rating"))) \
    .withColumn("Is_Active", F.col("Is_Active").cast("boolean"))

riders_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.silver.riders_clean")

print("Riders silver done")

# -----------------------------
# Drivers Cleaning
# -----------------------------
drivers_silver = spark.table("workspace.bronze.drivers_raw") \
    .dropDuplicates(["Driver_ID"]) \
    .withColumn("Driver_Rating",
        F.when(F.col("Driver_Rating").isNull(), F.lit(0.0)).otherwise(F.col("Driver_Rating"))) \
    .withColumn("Is_Available", F.col("Is_Available").cast("boolean"))

drivers_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.silver.drivers_clean")

print("Drivers silver done")

# -----------------------------
# Trips Cleaning + Incremental Logic
# -----------------------------
city_map = {
    "kolkata": "Kolkata",
    "HYD": "Hyderabad",
    "BLR": "Bangalore",
    "delhi": "Delhi",
    "mumbai": "Mumbai",
    "chennai": "Chennai"
}

city_expr = F.create_map([F.lit(x) for pair in city_map.items() for x in pair])

trips_cleaned = spark.table("workspace.bronze.trips_raw") \
    .withColumn("Trip_Start_Time", F.try_to_timestamp(F.col("Trip_Start_Time"))) \
    .withColumn("Trip_End_Time", F.try_to_timestamp(F.col("Trip_End_Time"))) \
    .withColumn("Last_Updated", F.try_to_timestamp(F.col("Last_Updated"))) \
    .withColumn("Pickup_City",
        F.coalesce(city_expr[F.col("Pickup_City")], F.col("Pickup_City"))) \
    .withColumn("Drop_City",
        F.coalesce(city_expr[F.col("Drop_City")], F.col("Drop_City"))) \
    .withColumn("Fare_Amount",
        F.when(F.col("Fare_Amount").isNull(), F.lit(0.0)).otherwise(F.col("Fare_Amount"))) \
    .withColumn("Distance_KM",
        F.when(F.col("Distance_KM").isNull(), F.lit(0.0)).otherwise(F.col("Distance_KM"))) \
    .withColumn("Trip_Status",
        F.upper(F.trim(F.col("Trip_Status")))) \
    .withColumn("Payment_Method",
        F.initcap(F.trim(F.col("Payment_Method"))))

# -----------------------------
# Deduplication using Window
# -----------------------------
window = Window.partitionBy("Trip_ID").orderBy(F.desc("Last_Updated"))

trips_latest = trips_cleaned \
    .withColumn("rn", F.row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")

# -----------------------------
# UPSERT (MERGE)
# -----------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.silver.trips_clean
USING DELTA
AS SELECT * FROM workspace.bronze.trips_raw WHERE 1=0
""")

DeltaTable.forName(spark, "workspace.silver.trips_clean") \
    .alias("target") \
    .merge(
        trips_latest.alias("source"),
        "target.Trip_ID = source.Trip_ID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print("Trips silver done (Incremental upsert applied)")