from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog
import getpass
import findspark

def spark_session(username):
    findspark.init()
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Bikewise'). \
        master('yarn'). \
        getOrCreate()
    spark.conf.set('spark.sql.shuffle.partition', 10)
#     print(spark)
    return (spark)

# if __name__ == "__main__":
#     username = getpass.getuser()
#     findspark.init()
#     spark_session(username)