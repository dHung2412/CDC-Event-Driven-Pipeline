from config.spark_config import SparkConnect, get_spark_config
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, when, concat_ws, regexp_replace, coalesce, trim, regexp_extract, expr, lit, current_timestamp
from spark.spark_write_data import SparkWriteDatabase
import os

def main():
    # Setup
    # packages = [
    #     "mysql:mysql-connector-java:8.0.33",
    #     "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    # ]

    packages = [
        "mysql:mysql-connector-java:8.0.33",
    ]

    spark_manager = SparkConnect(
        app_name='Logistic_ETL',
        master_url="local[*]",
        jar_packages=packages,
        log_level="WARN"
    )
    print("_______________ Spark Session Started _______________")
    spark = spark_manager.spark

    # Data Path
    csv_path = r"D:\Project\Data_Engineering\DE_ETL\src\data\logistic_data.csv"

    # Read CSV
    df_raw = spark.read.option("header", "true").csv(csv_path)

    # Data Transformation
    print("_______________ Data Transformation _______________")
    df_transformed = df_raw.select(
        col("Booking ID").alias("booking_id"),
        col("Shipment Type").alias("shipment_type"),
        col("Booking Date").alias("booking_date_raw"),
        col("Vehicle Registration").alias("vehicle_no"),
        col("Vehicle Type").alias("vehicle_type"),
        col("Origin Location").alias("origin"),
        col("Destination Location").alias("destination"),
        col("Origin Location Latitude").cast("double").alias("lat_origin"),
        col("Origin Location Longitude").cast("double").alias("long_origin"),
        col("Destination Location Latitude").cast("double").alias("lat_destination"),
        col("Destination Location Longitude").cast("double").alias("long_destination"),
        col("Gps Provider").alias("gps_provider"),
        col("Data Ping time").alias("data_ping_time"),
        col("Current Location Latitude").cast("double").alias("current_lat"),
        col("Curren Location Longitude").cast("double").alias("current_long"),
        col("Planned ETA").alias("planned_eta_raw"),
        col("Actual ETA").alias("actual_eta_raw"),
        col("Ontime").alias("ontime_raw"),
        col("Trip Start Date").alias("trip_start_raw"),
        col("Trip End Date").alias("trip_end_raw"),
        col("Transportation Distance (KM)").cast("int").alias("distance_km"),
        col("Minimum Kms To Be Covered In A Day").cast("int").alias("min_kms_day"),
        col("Driver Name").alias("driver_name"),
        col("Driver Mobile No").alias("driver_mobile"),
        col("Customer Name").alias("customer"),
        col("Supplier Name").alias("supplier"),
        col("Material Shipped").alias("material")
    )

    def parse_dt(c):
        return coalesce(
            to_timestamp(c, "M/d/yyyy H:mm"),
            to_timestamp(c, "M/d/yyyy h:mm a"),
            to_timestamp(c, "M/d/yyyy h:mm:ss a"),
            to_timestamp(c, "yyyy-MM-dd HH:mm:ss")
        )

    # 1. Clean Trip End Date (often has artifacts)
    df_cleaned = df_transformed.withColumn("trip_end_clean", regexp_replace(col("trip_end_raw"), r"[A-Za-z]", "")) \
        .withColumn("trip_end_clean", regexp_replace(col("trip_end_clean"), r":+", ":")) \
        .withColumn("trip_end_clean", trim(regexp_replace(col("trip_end_clean"), r"\s+", " ")))

    # 2. Process timestamps
    df_parsed = df_cleaned \
        .withColumn("booking_date", to_timestamp(col("booking_date_raw"), "M/d/yyyy")) \
        .withColumn("trip_start", parse_dt(col("trip_start_raw"))) \
        .withColumn("trip_end", parse_dt(col("trip_end_clean"))) \
        .withColumn("actual_eta", parse_dt(col("actual_eta_raw"))) \
        .withColumn("ontime", when(col("ontime_raw") == "Yes", True).otherwise(False)) \
        .withColumn("distance_km", col("distance_km").cast(IntegerType()))

    # 3. Special Logic for Planned ETA (Booking Date + Planned Time)
    df_parsed = df_parsed \
        .withColumn("p_hour_str", regexp_extract(col("planned_eta_raw"), r"(\d+):", 1)) \
        .withColumn("p_min_str", regexp_extract(col("planned_eta_raw"), r":(\d+)", 1)) \
        .withColumn("p_period", regexp_extract(col("planned_eta_raw"), r"(AM|PM)", 1)) \
        .withColumn("p_hour", col("p_hour_str").cast("int")) \
        .withColumn("p_hour_24", when(col("p_period") == "PM", col("p_hour") + 12).otherwise(col("p_hour"))) \
        .withColumn("planned_eta", expr("booking_date + make_interval(0,0,0,0, coalesce(p_hour_24,0), coalesce(p_min_str,0), 0)"))

    # 4. Final Selection and Deduplication
    base_cols = [
        "booking_id", "shipment_type", "booking_date", "vehicle_no", "vehicle_type",
        "origin", "destination", "lat_origin", "long_origin", "lat_destination", "long_destination",
        "distance_km", "gps_provider", "data_ping_time", "current_lat", "current_long",
        "planned_eta", "actual_eta", "ontime", "trip_start", "trip_end",
        "driver_name", "driver_mobile", "customer", "supplier", "material", "min_kms_day"
    ]
    
    df_final = df_parsed.select(*base_cols).dropDuplicates(["booking_id"])

    # 5. Split and Add Audit Columns
    df_active = df_final.filter(col("trip_end").isNull()).withColumn("updated_at", current_timestamp())
    df_history = df_final.filter(col("trip_end").isNotNull())

    print(f"---------->>> [DEBUG]: Stats - Active: {df_active.count()} | History: {df_history.count()}")

    # 6. Write to MySQL (Transactional Layer)
    spark_conf = get_spark_config() 
    db_writer = SparkWriteDatabase(spark, spark_conf)
    
    db_writer.write_all(df_active, df_history)

    print("_______________ Data pushed to MySQL Binlog Source _______________")
    spark_manager.stop()

if __name__ == "__main__":
    main()
