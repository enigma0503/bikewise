from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog

def spark_session(username):
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Bikewise'). \
        master('yarn'). \
        getOrCreate()
    spark.conf.set('spark.sql.shuffle.partition', 10)
    return (spark)