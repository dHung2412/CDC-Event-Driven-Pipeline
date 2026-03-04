from config.spark_config import SparkConnect, get_spark_config
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, when, concat_ws, regexp_replace, coalesce, trim, regexp_extract, expr, lit, current_timestamp
from spark.spark_write_data import SparkWriteDatabase
import os

def main():
    # Setup
    packages = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    ]

    spark_manager = SparkConnect(
        app_name='Logistic_ETL_Spark',
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
        col("Booking Date").alias("booking_date"),
        col("Vehicle Registration").alias("vehicle_no"),
        col("Vehicle Type").alias("vehicle_type"),
        col("Origin Location").alias("origin"),
        col("Destination Location").alias("destination"),
        col("Planned ETA").alias("planned_eta"),
        col("Actual ETA").alias("actual_eta"),
        col("Ontime").alias("ontime"),
        col("Trip Start Date").alias("trip_start"),
        col("Trip End Date").alias("trip_end"),
        col("Transportation Distance (KM)").alias("distance_km"),
        col("Driver Name").alias("driver_name"),
        col("Driver Mobile No").alias("driver_mobile"),
        col("Customer Name").alias("customer"),
        col("Supplier Name").alias("supplier"),
        col("Material Shipped").alias("material")
    )

    # --- 1
    df_cleaned = df_transformed.withColumn("trip_end_clean", regexp_replace(col("trip_end"), r"[A-Za-z]", "")) \
    .withColumn("trip_end_clean",regexp_replace(col("trip_end_clean"), r":+", ":")) \
    .withColumn("trip_end_clean",(regexp_replace(col("trip_end_clean"), r"\s+", " ")))

    df_cleaned = df_cleaned.withColumn("p_hours", lit(12)) \
    .withColumn("p_minutes", regexp_extract(col("planned_eta"), r"(\d+):(\d+)", 1).cast("int")) \
    .withColumn("p_seconds", regexp_extract(col("planned_eta"), r"(\d+):(\d+)", 2).cast("int"))

    # --- 2
    
    def parse_dt(col):
        return coalesce(
            to_timestamp(col, "M/d/yyyy H:mm"),
            to_timestamp(col, "M/d/yyyy H:mm:ss"),
            to_timestamp(col, "M/d/yyyy h:mm a"),
            to_timestamp(col, "M/d/yyyy h:mm:ss a"),
            to_timestamp(col, "yyyy-MM-dd HH:mm:ss")
        )

    # planned_eta = booking_date (00:00) + (12h + p_minutes + p_seconds)
    df_parsed_data = df_cleaned \
    .withColumn("booking_date_ts", to_timestamp(col("booking_date"), "M/d/yyyy")) \
    .withColumn("trip_start", parse_dt(col("trip_start"))) \
    .withColumn("trip_end", parse_dt(col("trip_end_clean"))) \
    .withColumn("actual_eta", parse_dt(col("actual_eta"))) \
    .withColumn("planned_eta", expr("booking_date_ts + make_interval(0,0,0,0, p_hours, coalesce(p_minutes,0), coalesce(p_seconds,0))")) \
    .withColumn("ontime", when(col("ontime") == "Yes", True).otherwise(False)) \
    .withColumn("distance_km", col("distance_km").cast(IntegerType())) \
    .fillna(0, subset=["distance_km"])

    df_final_data = df_parsed_data.withColumn("booking_date", col("booking_date_ts"))

    # --- 3
    base_cols = [
        "booking_id", "shipment_type", "booking_date", "vehicle_no", "vehicle_type",
        "origin", "destination", "planned_eta", "actual_eta", "ontime",
        "trip_start", "trip_end", "distance_km", "driver_name", "driver_mobile",
        "customer", "supplier", "material"
    ]
    # --- Deduplicate
    df_final = df_final_data.select(*base_cols).dropDuplicates(["booking_id"])

    # --- Split data
    df_active = df_final.filter(col("trip_end").isNull()).withColumn("updated_at", current_timestamp())

    df_history = df_final.filter(col("trip_end").isNotNull())

    print(f"---------->>> [DEBUG]: Stats - Active: {df_active.count()} | History: {df_history.count()}")

    # --- Write to Databases
    spark_conf = get_spark_config() 
    db_writer = SparkWriteDatabase(spark, spark_conf)
    
    db_writer.write_all(df_active, df_history)

    print(f"_______________ETL Summary_______________")
    print(f"_____Total Records: {df_final.count()}_____")
    print(f"_____Active Records: {df_active.count()}_____")
    print(f"_____History Records: {df_history.count()}_____")

    spark_manager.stop()

if __name__ == "__main__":
    main()
