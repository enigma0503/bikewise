from pyspark.sql.functions import *
from pyspark.sql.window import Window


def unique_records(hdfs_dir, spark, hdfs_username):
    df = spark.read. \
        parquet(f'{hdfs_dir}/{hdfs_username}/warehouse/{hdfs_username}_bikewise_final.db/incidents_final')

    spec = Window. \
        partitionBy('id'). \
        orderBy(col("ts").desc())

    df_temp = df. \
        withColumn('ts', unix_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias('ts')). \
        withColumn("rank", dense_rank().over(spec)). \
        filter('rank = 1')

    df_temp = df_temp.drop('ts', 'rank')
    df_temp.write.mode('overwrite').saveAsTable(f'{hdfs_username}_bikewise_final.incidents_unique')
