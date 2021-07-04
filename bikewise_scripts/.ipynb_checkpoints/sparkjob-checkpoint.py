import spark_session
import create_db
import create_dataframe
import write_into_table
import write_json
import report
import getpass
import datetime
import time
from pyspark.sql.functions import *


if __name__ == "__main__":
    
    hdfs_username = 'itv000579'
    username = getpass.getuser()
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")
    
    spark = spark_session.spark_session(username)
    
    print("spark_session", spark.sql('select current_database()'))
    create_db.create_db(spark,username)
    
    file_path = f'/user/{hdfs_username}/bikewise/raw/{yesterday}/{yesterday}.json'
    
    df = create_dataframe.dataframe(spark, file_path)
    
    write_into_table.insert_into_table(spark, hdfs_username, df, 'raw')
    
    df_init = df. \
    select('id', 'type','title', 'description', 'location_type',
       'location_description', 'media.image_url', 'occurred_at','updated_at', 
        'type_properties', 'year', 'month', 'day')
    
    write_json.df_to_json(today, hdfs_username, df_init, 'initial')
    
    write_into_table.insert_into_table(spark, hdfs_username, df_init, 'initial')
    
    df_final = df_init. \
    select('id', 'type','title', 'description',
       'location_description', 'image_url', 
       'occurred_at','updated_at', 'year', 'month', 'day'). \
    withColumn('occurred_at', from_unixtime('occurred_at', "yyyy-MM-dd HH:mm:ss")). \
    withColumn('updated_at', from_unixtime('updated_at', "yyyy-MM-dd HH:mm:ss"))
    
    write_json.df_to_json(yesterday, hdfs_username, df_final, 'final')
    
    write_into_table.insert_into_table(spark, hdfs_username, df_final, 'final')
    
    report.create_report(yesterday, df_final)