from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog
import getpass
import findspark


def spark_session(hdfs_dir, username):
    findspark.init()
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"{hdfs_dir}/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Bikewise'). \
        master('yarn'). \
        getOrCreate()
    spark.conf.set('spark.sql.shuffle.partition', 10)
    return spark
