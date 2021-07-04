import datetime
import time
import json
import requests
import pandas as pd
import os
import getpass
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

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


def create_db(spark, username):
    spark.sql(f'create database if not exists {username}_bikewise_raw')
    spark.sql(f'create database if not exists {username}_bikewise_initial')
    spark.sql(f'create database if not exists {username}_bikewise_final')
    

def dataframe(spark, file_path):
    df_raw = spark. \
    read. \
    json(file_path)

    df_raw = df_raw. \
    withColumn('year', date_format(current_date(), 'yyyy')). \
    withColumn('month', date_format(current_date(), 'MM')). \
    withColumn('day', date_format(current_date(), 'dd'))
    return(df_raw)
    
    
def insert_into_table(spark, hdfs_username, df, table_name):
    tables = spark.catalog.listTables(f'{hdfs_username}_bikewise_{table_name}')
    found = False
    for table in tables:
        if(list(table)[0] == f'incidents_{table_name}'):
            found = True
            break
    if(found):
        df. \
        write. \
        mode('append'). \
        partitionBy('year', 'month', 'day'). \
        parquet(f'/user/{hdfs_username}/warehouse/{hdfs_username}_bikewise_{table_name}.db/incidents_{table_name}')

        spark.sql(f'''MSCK REPAIR TABLE 
              {hdfs_username}_bikewise_{table_name}.incidents_{table_name}''')
    else:
        df. \
        write. \
        partitionBy('year', 'month', 'day'). \
        saveAsTable(f'{hdfs_username}_bikewise_{table_name}.incidents_{table_name}')
        
        
def df_to_json(today, hdfs_username, df, table_name):
    df.write.mode('overwrite').format('json').save(f'/user/{hdfs_username}/bikewise/{table_name}/{today}')        
    
        
def create_report(today, df):
    df = df.select('type'). \
        groupBy(col('type')).count()
    
    df = df.toPandas()
    
    graph = plt.figure(figsize=(10, 8))
    splot=sns.barplot(x="type",y="count",data = df)
    for p in splot.patches:
        splot.annotate(format(p.get_height(), '.0f'), 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points')
    plt.xlabel("Incident Type", size=14)
    plt.ylabel("Count", size=14)
    plt.title("Count of Incidents", size = 20)
    plt.savefig(f'/home/itv000579/shubham/bike_data/reports/report_{today}.pdf')
    plt.close()
    
    
    
if __name__ == "__main__":

    hdfs_username = 'itv000579'
    username = getpass.getuser()
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    
    spark = spark_session(username)
    
    create_db(spark, username)
    
    file_path = f'/user/{hdfs_username}/bikewise/raw/{today}/{today}.json'
    
    df = dataframe(spark, file_path)
    
    insert_into_table(spark, hdfs_username, df, 'raw')
    
    df_init = df. \
    select('id', 'type','title', 'description', 'location_type',
       'location_description', 'media.image_url', 'occurred_at','updated_at', 
        'type_properties', 'year', 'month', 'day')
    
    df_to_json(today, hdfs_username, df_init, 'initial')
    
    insert_into_table(spark, hdfs_username, df_init, 'initial')
    
    
    df_final = df_init. \
    select('id', 'type','title', 'description',
       'location_description', 'image_url', 
       'occurred_at','updated_at', 'year', 'month', 'day'). \
    withColumn('occurred_at', from_unixtime('occurred_at', "yyyy-MM-dd HH:mm:ss")). \
    withColumn('updated_at', from_unixtime('updated_at', "yyyy-MM-dd HH:mm:ss"))
    
    
    df_to_json(today, hdfs_username, df_final, 'final')
    
    
    insert_into_table(spark, hdfs_username, df_final, 'final')
    
    
    create_report(today, df_final)
    
#     t18 = threading.Thread(target = copy_report, args = (timestamps, hdfs_username, queue))
#     t18.start()
#     t18.join()
#     file_location = queue.get()
#     logging.info("Report copied to HDFS at the location: ", file_location)
    
    print("Process completed")