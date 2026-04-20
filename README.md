# 🚀 Ride-Sharing Incremental Data Pipeline

## 📌 Overview
This project implements an end-to-end incremental data pipeline for a ride-sharing system using PySpark and Databricks. It processes daily trip data, handles updates, and generates business insights.

---

## 🏗 Architecture
The pipeline follows Medallion Architecture:

- Bronze Layer → Raw data ingestion  
- Silver Layer → Data cleaning + incremental processing  
- Gold Layer → Analytics & reporting  

---

## 🟫 Bronze Layer (Raw Data Ingestion)

In the Bronze layer, raw datasets are ingested without any transformations.

### What I implemented:
- Loaded **Riders, Drivers, and Trips (7 daily files)** from DBFS
- Added metadata columns:
  - `Load_Date`
  - `File_Name`
- Stored data as Delta tables:
  - `workspace.bronze.riders_raw`
  - `workspace.bronze.drivers_raw`
  - `workspace.bronze.trips_raw`
- Used **append mode** for incremental trip data ingestion

---

## ⚙️ Silver Layer (Data Cleaning & Incremental Processing)

The Silver layer focuses on cleaning data and implementing incremental logic.

### Data Cleaning:
- Handled missing values:
  - Fare → 0  
  - Distance → 0  
- Standardized city names using mapping
- Cleaned categorical fields (Trip_Status, Payment_Method)
- Converted timestamp columns using `try_to_timestamp`

### Deduplication:
- Used **Window Function**:
- ## 🟨 Gold Layer (Analytics & Reporting)

The Gold layer is designed to create business-ready datasets and generate analytical reports from cleaned Silver data.

### Data Integration:
- Joined datasets:
  - Trips (fact table)
  - Riders (dimension)
  - Drivers (dimension)

### Join Conditions:
- Trips.Rider_ID = Riders.Rider_ID  
- Trips.Driver_ID = Drivers.Driver_ID  

### Transformations Performed:
- Renamed columns to avoid conflicts (Rider_City, Driver_City, etc.)
- Created derived column:
  - `Trip_Date` from Trip_Start_Time

### Final Gold Table:
- `workspace.gold.trips_enriched`

---

## 📊 Reports Generated (18 Total)

### Core Reports:
- Daily Revenue (Completed trips only)
- Trip Status Distribution
- Driver Performance (Trips & Earnings)
- City-wise Demand
- Trip Insights (Avg Distance & Fare)

### Incremental Analysis:
- Day-over-Day Revenue Change
- Trip Growth Trend
- Cancellation Rate Analysis
- Late Data Impact (updates to past records)

### Advanced Analytics:
- Payment Distribution (Trips & Revenue share)
- Daily Payment Usage Trends
- Preferred Payment Method per Rider
- Driver Earnings by Payment Type
- City-wise Payment Preferences
- Cancellation Rate by Payment Method
- Average Fare by Payment Method
- Repeat Payment Usage Behavior
- Peak Hour Payment Trends

---

### Output:
- All 18 reports were generated as Delta tables
- Reports were exported into a single Excel file:
  - `all_reports.xlsx`
```sql


ROW_NUMBER() OVER (PARTITION BY Trip_ID ORDER BY Last_Updated DESC)
