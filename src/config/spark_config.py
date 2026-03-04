import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from config.database_config import get_database_config
from typing import Optional, List, Dict

class SparkConnect:
    def __init__(
        self,
        app_name:str,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = "2g",
        executor_cores: Optional[int] = 2,
        driver_memory: Optional[str] = "2g",
        num_executor: Optional[int] = 2,
        jar_packages: Optional[List[str]] = None,
        log_level: str = "WARN",
        spark_conf: Optional[Dict[str, str]] = None
    ):
        self.app_name = app_name
        self.master_url = master_url
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.num_executor = num_executor
        self.jar_packages = jar_packages
        self.log_level = log_level
        self.spark_conf = spark_conf
        self.spark = self.create_spark_session()

    def create_spark_session(self) -> SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master_url) \
            .config("spark.executor.memory", self.executor_memory) \
            .config("spark.executor.cores", self.executor_cores) \
            .config("spark.driver.memory", self.driver_memory) \
            .config("spark.executor.instances", self.num_executor) \
            .config("spark.pyspark.python", sys.executable) \
            .config("spark.pyspark.driver.python", sys.executable) \
            .config("spark.sql.session.timeZone", "Asia/Bangkok") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        if self.jar_packages:
            jar_packages_url = ",".join([jar_package for jar_package in self.jar_packages])
            builder.config("spark.jars.packages", jar_packages_url)
        if self.spark_conf:
            for key, value in self.spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()   
            print("_______Stop SparkSession_______")


def get_spark_config() -> Dict :
    db_config = get_database_config()

    return {
        "mysql" : {
            "table_active" : db_config["mysql"].table_active,
            "table_history" : db_config["mysql"].table_history,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(
                db_config["mysql"].host, db_config["mysql"].port, db_config["mysql"].database
            ),
            "config" : {
                "host" : db_config["mysql"].host,
                "port" : db_config["mysql"].port,
                "user": db_config["mysql"].user,
                "password": db_config["mysql"].password,
                "database": db_config["mysql"].database,
            }
        },
        "mongodb" : {
            "uri": db_config["mongodb"].uri,
            "database" : db_config["mongodb"].database,
            "collection" : db_config["mongodb"].collection,
            "package_path": db_config["mongodb"].jar_path
        },
        "redis" : {
            "host" : db_config["redis"].host,
            "port" : db_config["redis"].port,
            "user" : db_config["redis"].user,
            "password" : db_config["redis"].password,
            "database" : db_config["redis"].database
        }
    }

if __name__ == "__main__":
    spark_conf = get_spark_config()
    print(spark_conf)

