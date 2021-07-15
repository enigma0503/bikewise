import bookmark
import spark_session
import create
import write_data
import getpass
from unique import unique_records
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog

if __name__ == "__main__":
    
    hdfs_username = 'itv000579'
    username = getpass.getuser()
    
    ts_list = bookmark.get_bookmark(5)
    
    spark = spark_session.spark_session(username)
    
    create.create_db(spark,username) 
    
    for timestamps in ts_list:
        if(timestamps[3]==1 and timestamps[4]==1):
            yesterday = timestamps[2]
            file_path = f'/user/{hdfs_username}/bikewise/raw/{yesterday}/{yesterday}.json'

            df = create.raw_df(spark, file_path)

            write_data.insert_into_table(spark, hdfs_username, df, 'raw', timestamps)

            df_init = create.init_df(df)

            write_data.df_to_json(yesterday, hdfs_username, df_init, 'initial')

            write_data.insert_into_table(spark, hdfs_username, df_init, 'initial', timestamps)

            df_final = create.final_df(df_init)

            write_data.df_to_json(yesterday, hdfs_username, df_final, 'final')

            write_data.insert_into_table(spark, hdfs_username, df_final, 'final', timestamps)

            create.create_report(yesterday, df_final)

            unique_records(spark, hdfs_username)

            SparkSession.stop(spark)