# -----------------------------
# GOLD LAYER
# REPORTS (1 to 18)
# -----------------------------

# Report 1: Daily Revenue
# Calculates total revenue per day (only completed trips)

# Report 2: Trip Status Distribution
# Counts number of trips by status (Completed, Cancelled, etc.)

# Report 3: Driver Performance
# Calculates total trips and total earnings per driver

# Report 4: City-wise Demand
# Counts number of trips originating from each city

# Report 5: Trip Insights
# Calculates average distance and average fare

# Report 6: Day-over-Day Revenue Change
# Compares daily revenue with previous day and shows change

# Report 7: Trip Growth Trend
# Calculates daily growth percentage in number of trips

# Report 8: Cancellation Rate Analysis
# Calculates percentage of cancelled trips per day

# Report 9: Late Data Impact
# Identifies trips updated after their actual trip date

# Report 10: Payment Distribution
# Shows number of trips and revenue contribution by payment method

# Report 11: Daily Payment Usage
# Tracks payment method usage trends per day

# Report 12: Preferred Payment per Rider
# Identifies most frequently used payment method for each rider

# Report 13: Driver Earnings by Payment Type
# Shows how much each driver earns via each payment method

# Report 14: City-wise Payment Preferences
# Shows preferred payment methods across cities

# Report 15: Cancellation by Payment Method
# Analyzes cancellation rate based on payment method

# Report 16: Average Fare by Payment Method
# Calculates average fare for each payment type

# Report 17: Repeat Payment Usage Behavior
# Analyzes whether riders stick to one payment method or switch

# Report 18: Peak Hour Payment Trends
# Analyzes payment method usage across different hours of the day

from pyspark.sql import functions as F

trips   = spark.table("workspace.silver.trips_clean")
riders  = spark.table("workspace.silver.riders_clean")
drivers = spark.table("workspace.silver.drivers_clean")

riders_renamed = riders \
    .drop("Load_Date", "File_Name") \
    .withColumnRenamed("City",         "Rider_City") \
    .withColumnRenamed("Rating",       "Rider_Rating") \
    .withColumnRenamed("Is_Active",    "Rider_Is_Active") \
    .withColumnRenamed("Last_Updated", "Rider_Last_Updated") \
    .withColumnRenamed("Signup_Date",  "Rider_Signup_Date")

drivers_renamed = drivers \
    .drop("Load_Date", "File_Name") \
    .withColumnRenamed("City",         "Driver_City") \
    .withColumnRenamed("Is_Available", "Driver_Is_Available") \
    .withColumnRenamed("Last_Updated", "Driver_Last_Updated") \
    .withColumnRenamed("Joining_Date", "Driver_Joining_Date")

gold = trips \
    .join(riders_renamed,  "Rider_ID",  "left") \
    .join(drivers_renamed, "Driver_ID", "left") \
    .withColumn("Trip_Date", F.to_date("Trip_Start_Time"))

gold.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.trips_enriched")

print("Gold base table done:", gold.count(), "rows ")

from pyspark.sql import functions as F

gold = spark.table("workspace.gold.trips_enriched")

# Report 1: Daily Revenue
report1 = gold.filter(F.col("Trip_Status") == "COMPLETED") \
    .groupBy("Trip_Date") \
    .agg(F.round(F.sum("Fare_Amount"), 2).alias("Total_Revenue")) \
    .orderBy("Trip_Date")

report1.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report1_daily_revenue")
print("Report 1 done ")

# Report 2: Trip Status Distribution
report2 = gold.groupBy("Trip_Status") \
    .agg(F.count("Trip_ID").alias("Trip_Count"))

report2.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report2_status_distribution")
print("Report 2 done ")

# Report 3: Driver Performance
report3 = gold.groupBy("Driver_ID", "Driver_Name") \
    .agg(
        F.count("Trip_ID").alias("Total_Trips"),
        F.round(F.sum("Fare_Amount"), 2).alias("Total_Earnings")
    ).orderBy(F.desc("Total_Earnings"))

report3.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report3_driver_performance")
print("Report 3 done ")

# Report 4: City-wise Demand
report4 = gold.groupBy("Pickup_City") \
    .agg(F.count("Trip_ID").alias("Trip_Count")) \
    .orderBy(F.desc("Trip_Count"))

report4.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report4_city_demand")
print("Report 4 done ")

# Report 5: Trip Insights
report5 = gold.agg(
    F.round(F.avg("Distance_KM"), 2).alias("Avg_Distance_KM"),
    F.round(F.avg("Fare_Amount"),  2).alias("Avg_Fare")
)

report5.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report5_trip_insights")
print("Report 5 done ")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold = spark.table("workspace.gold.trips_enriched")
w = Window.orderBy("Trip_Date")

# Report 6: Day-over-Day Revenue Change
daily_rev = gold.filter(F.col("Trip_Status") == "COMPLETED") \
    .groupBy("Trip_Date") \
    .agg(F.round(F.sum("Fare_Amount"), 2).alias("Revenue"))

report6 = daily_rev \
    .withColumn("Prev_Revenue", F.lag("Revenue").over(w)) \
    .withColumn("Revenue_Diff",
        F.round(F.col("Revenue") - F.col("Prev_Revenue"), 2)) \
    .withColumn("Revenue_Change_Pct",
        F.round((F.col("Revenue_Diff") / F.col("Prev_Revenue")) * 100, 2)) \
    .orderBy("Trip_Date")

report6.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report6_dod_revenue")
print("Report 6 done ")

# Report 7: Trip Growth Trend
daily_trips = gold.groupBy("Trip_Date") \
    .agg(F.count("Trip_ID").alias("Trip_Count"))

report7 = daily_trips \
    .withColumn("Prev_Count", F.lag("Trip_Count").over(w)) \
    .withColumn("Growth_Pct",
        F.round(((F.col("Trip_Count") - F.col("Prev_Count")) / F.col("Prev_Count")) * 100, 2)) \
    .orderBy("Trip_Date")

report7.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report7_trip_growth")
print("Report 7 done ")

# Report 8: Cancellation Rate per Day
report8 = gold.groupBy("Trip_Date") \
    .agg(
        F.count("Trip_ID").alias("Total_Trips"),
        F.sum(F.when(F.col("Trip_Status") == "CANCELLED", 1).otherwise(0)).alias("Cancelled_Trips")
    ) \
    .withColumn("Cancellation_Rate_Pct",
        F.round((F.col("Cancelled_Trips") / F.col("Total_Trips")) * 100, 2)) \
    .orderBy("Trip_Date")

report8.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report8_cancellation_rate")
print("Report 8 done ")

# Report 9: Late Data Impact
report9 = gold \
    .filter(F.to_date("Last_Updated") > F.col("Trip_Date")) \
    .select("Trip_ID", "Trip_Date", "Last_Updated", "Trip_Status", "Fare_Amount") \
    .withColumn("Days_Late",
        F.datediff(F.to_date("Last_Updated"), F.col("Trip_Date"))) \
    .orderBy(F.desc("Days_Late"))

report9.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report9_late_data")
print("Report 9 done ")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold = spark.table("workspace.gold.trips_enriched")

# Report 10: Payment Distribution
total_trips = gold.count()
report10 = gold.groupBy("Payment_Method") \
    .agg(
        F.count("Trip_ID").alias("Trip_Count"),
        F.round(F.sum("Fare_Amount"), 2).alias("Total_Revenue")
    ) \
    .withColumn("Pct_Share",
        F.round((F.col("Trip_Count") / total_trips) * 100, 2)) \
    .orderBy(F.desc("Trip_Count"))

report10.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report10_payment_distribution")
print("Report 10 done ")

# Report 11: Daily Payment Usage
report11 = gold.groupBy("Trip_Date", "Payment_Method") \
    .agg(
        F.count("Trip_ID").alias("Trip_Count"),
        F.round(F.sum("Fare_Amount"), 2).alias("Daily_Revenue")
    ).orderBy("Trip_Date", "Payment_Method")

report11.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report11_daily_payment_trend")
print("Report 11 done ")

# Report 12: Preferred Payment per Rider
w_rider = Window.partitionBy("Rider_ID").orderBy(F.desc("Trip_Count"))
report12 = gold.groupBy("Rider_ID", "Rider_Name", "Payment_Method") \
    .agg(F.count("Trip_ID").alias("Trip_Count")) \
    .withColumn("rn", F.row_number().over(w_rider)) \
    .filter("rn = 1") \
    .drop("rn") \
    .orderBy("Rider_ID")

report12.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report12_rider_payment_preference")
print("Report 12 done ")

# Report 13: Driver Earnings by Payment Type
report13 = gold.groupBy("Driver_ID", "Driver_Name", "Payment_Method") \
    .agg(F.round(F.sum("Fare_Amount"), 2).alias("Total_Earnings")) \
    .orderBy("Driver_ID", "Payment_Method")

report13.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report13_driver_earnings_by_payment")
print("Report 13 done ")

# Report 14: City-wise Payment Preferences
report14 = gold.groupBy("Pickup_City", "Payment_Method") \
    .agg(F.count("Trip_ID").alias("Trip_Count")) \
    .orderBy("Pickup_City", F.desc("Trip_Count"))

report14.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report14_city_payment_preference")
print("Report 14 done ")

# Report 15: Cancellation by Payment Method
report15 = gold.groupBy("Payment_Method") \
    .agg(
        F.count("Trip_ID").alias("Total_Trips"),
        F.sum(F.when(F.col("Trip_Status") == "CANCELLED", 1).otherwise(0)).alias("Cancelled_Trips")
    ) \
    .withColumn("Cancellation_Rate_Pct",
        F.round((F.col("Cancelled_Trips") / F.col("Total_Trips")) * 100, 2)) \
    .orderBy(F.desc("Cancellation_Rate_Pct"))

report15.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report15_cancellation_by_payment")
print("Report 15 done ")

# Report 16: Avg Fare by Payment Method
report16 = gold.groupBy("Payment_Method") \
    .agg(F.round(F.avg("Fare_Amount"), 2).alias("Avg_Fare")) \
    .orderBy(F.desc("Avg_Fare"))

report16.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report16_avg_fare_by_payment")
print("Report 16 done ")

# Report 17: Repeat Payment Usage Behavior
w_repeat = Window.partitionBy("Rider_ID").orderBy(F.desc("Usage_Count"))
report17 = gold.groupBy("Rider_ID", "Rider_Name", "Payment_Method") \
    .agg(F.count("Trip_ID").alias("Usage_Count")) \
    .withColumn("Payment_Rank", F.row_number().over(w_repeat)) \
    .orderBy("Rider_ID", "Payment_Rank")

report17.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report17_repeat_payment_behavior")
print("Report 17 done ")

# Report 18: Peak Hour Payment Trends
report18 = gold \
    .withColumn("Hour_Of_Day", F.hour("Trip_Start_Time")) \
    .groupBy("Hour_Of_Day", "Payment_Method") \
    .agg(F.count("Trip_ID").alias("Trip_Count")) \
    .orderBy("Hour_Of_Day", F.desc("Trip_Count"))

report18.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.gold.report18_peak_hour_payment")
print("Report 18 done ")

import os

paths = ["/tmp/reports", "/local_disk0/reports", "/databricks/driver/tmp/reports"]

for path in paths:
    try:
        os.makedirs(path, exist_ok=True)
        test_file = f"{path}/test.txt"
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        print(f" Writable: {path}")
    except Exception as e:
        print(f"Not writable: {path} — {e}")

import pandas as pd
import os

os.makedirs("/tmp/reports", exist_ok=True)

reports = [
    "report1_daily_revenue",
    "report2_status_distribution",
    "report3_driver_performance",
    "report4_city_demand",
    "report5_trip_insights",
    "report6_dod_revenue",
    "report7_trip_growth",
    "report8_cancellation_rate",
    "report9_late_data",
    "report10_payment_distribution",
    "report11_daily_payment_trend",
    "report12_rider_payment_preference",
    "report13_driver_earnings_by_payment",
    "report14_city_payment_preference",
    "report15_cancellation_by_payment",
    "report16_avg_fare_by_payment",
    "report17_repeat_payment_behavior",
    "report18_peak_hour_payment",
]

output_path = "/tmp/reports/all_reports.xlsx"

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    for report in reports:
        df = spark.table(f"workspace.gold.{report}").toPandas()
        sheet_name = report[:31]  # use full name, just cap at 31 chars
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Written sheet: {sheet_name}")

print("Excel file created")

import shutil

shutil.copy(
    "/tmp/reports/all_reports.xlsx",
    "/Workspace/all_reports.xlsx"
)
print("File saved to Workspace")