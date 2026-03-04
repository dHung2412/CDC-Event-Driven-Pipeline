from typing import Dict
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession, DataFrame
import redis
from database.mysql_connect import MySQLConnect

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, spark_db_config: Dict):
        self.spark = spark
        self.spark_db_config = spark_db_config

    def spark_write_mysql(self, df_write: DataFrame, table_name: str, mode: str = "append"):
        mysql_conf = self.spark_db_config["mysql"]
        
        if mode == "overwrite":
            try:
                from config.database_config import get_database_config
                raw_config = get_database_config()
                with MySQLConnect(raw_config["mysql"]) as mysql_conn:
                    mysql_conn.cursor.execute(f"DELETE FROM {table_name}")
                    mysql_conn.connection.commit()
                mode = "append"
                print(f"---------->>> MySQLConnect: Deleted all records in {table_name}")
            except Exception as e:
                print(f"<<<---------- SQL Warning: Failed to manual delete {table_name}: {e}")

        print(f"[DEBUG-SPARK] Writing to JDBC URL: {mysql_conf['jdbc_url']} | Table: {table_name} | Mode: {mode}")
        df_write.write \
            .format("jdbc") \
            .option("url", mysql_conf["jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", mysql_conf["config"]["user"]) \
            .option("password", mysql_conf["config"]["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        
        # --- validate
        df_read_back = self.spark.read \
            .format("jdbc") \
            .option("url", mysql_conf["jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", mysql_conf["config"]["user"]) \
            .option("password", mysql_conf["config"]["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        source_count = df_write.count()
        target_count = df_read_back.count()
        sample_ids = [row.booking_id for row in df_read_back.select("booking_id").limit(3).collect()]
        print(f"[DEBUG-VALIDATE] Target Count: {target_count} | Sample IDs in DB: {sample_ids}")

        if source_count == target_count:
            print(f"----------> [VALIDATE SUCCESS] MySQL {table_name}: {target_count} records (Matches Source)")
        else:
            print(f"----------> [VALIDATE WARNING] MySQL {table_name}: Source({source_count}) and Target({target_count}) Don't Match")
            # missing = df_write.select("booking_id").subtract(df_read_back.select("booking_id"))
            # missing.show()

        print(f"_______________Write success to MySQL {table_name}_______________")

    # def spark_write_mongodb(self, df_write: DataFrame, mode: str = "append"):
    #     """
    #     Write to MongoDB using Upsert logic. 
    #     booking_id will be mapped to _id to ensure only one document per booking exists.
    #     """
    #     mongo_conf = self.spark_db_config["mongodb"]
        
    #     df_mongo = df_write.withColumn("_id", col("booking_id"))
        
    #     df_mongo.write \
    #         .format("mongo") \
    #         .option("uri", mongo_conf["uri"]) \
    #         .option("database", mongo_conf["database"]) \
    #         .option("collection", mongo_conf["collection"]) \
    #         .option("replaceDocument", "true") \
    #         .mode(mode) \
    #         .save()
        
    #     # --- validate
    #     df_read_back = self.spark.read \
    #         .format("mongo") \
    #         .option("uri", mongo_conf["uri"]) \
    #         .option("database", mongo_conf["database"]) \
    #         .option("collection", mongo_conf["collection"]) \
    #         .load()
        
    #     source_count = df_write.count()
    #     target_count = df_read_back.count()

    #     if source_count == target_count:
    #         print(f"----------> [VALIDATE SUCCESS] MongoDB {mongo_conf['collection']}: {target_count} records (Matches Source)")
    #     else:
    #         print(f"----------> [VALIDATE WARNING] MongoDB {mongo_conf['collection']}: Source({source_count}) and Target({target_count}) Don't Match")
            
    #     print(f"_______________Write success to MongoDB {mongo_conf['collection']}_______________")

    # def spark_write_redis(self, df_write: DataFrame):
    #     """
    #     Write active bookings to Redis as Hashes.
    #     Key format: booking:{booking_id}
    #     """
    #     redis_conf = self.spark_db_config["redis"]
        
    #     def send_to_redis(partition):
    #         # Create connection inside partition to avoid serialization issues
    #         r = redis.Redis(
    #             host=redis_conf["host"],
    #             port=redis_conf["port"],
    #             password=redis_conf["password"],
    #             db=redis_conf["database"],
    #             decode_responses=True
    #         )

    #         for row in partition:
    #             booking_id = row['booking_id']
    #             key = f"booking:{booking_id}"
    #             data = {k: str(v) for k, v in row.asDict().items() if v is not None}
    #             if data:
    #                 r.hset(key, mapping=data)
    #                 r.sadd("active_bookings", booking_id)
    #         r.close()

    #     df_write.foreachPartition(send_to_redis)
    #     print(f"_______________Write success to Redis_______________")

    def write_all(self, df_active: DataFrame, df_history: DataFrame):
        if df_active and not df_active.isEmpty():
            # Using overwrite for Active table to refresh current state
            self.spark_write_mysql(df_active, self.spark_db_config["mysql"]["table_active"], mode="overwrite")

        if df_history and not df_history.isEmpty():
            # Using append for History table to accumulate finished records
            self.spark_write_mysql(df_history, self.spark_db_config["mysql"]["table_history"], mode="append")
            # self.spark_write_mysql(df_history, self.spark_db_config["mysql"]["table_history"], mode="overwrite")

        print("_______________Write to SOT (MySQL) completed_______________")
