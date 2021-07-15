import bookmark
import spark_session
import create
import write_data
import getpass
import os
import config
from unique import unique_records
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog


if __name__ == "__main__":

    env = os.environ.get('ENVIRON')
    config_loc = os.environ.get('CONFIG_LOC')

    conf = config.get_config(config_loc, env)

    hdfs_username = conf['HDFS_USERNAME']
    bookmark_file = conf['BOOKMARK_FILE']
    local_data_dir = conf['LOCAL_DATA_DIR']
    hdfs_dir = conf['HDFS_DIR']
    local_reports_dir = conf['LOCAL_REPORTS_DIR']
    username = getpass.getuser()

    ts_list = bookmark.get_bookmark(bookmark_file, 5)

    spark = spark_session.spark_session(hdfs_dir, username)

    create.create_db(spark, username)

    for timestamps in ts_list:
        if timestamps[3] == 1 and timestamps[4] == 1:
            yesterday = timestamps[2]
            file_path = f'/user/{hdfs_username}/bikewise/raw/{yesterday}/{yesterday}.json'

            df = create.raw_df(spark, file_path)

            write_data.insert_into_table(bookmark_file, hdfs_dir, spark, hdfs_username, df, 'raw', timestamps)

            df_init = create.init_df(df)

            write_data.df_to_json(hdfs_dir, yesterday, hdfs_username, df_init, 'initial')

            write_data.insert_into_table(bookmark_file, hdfs_dir, spark, hdfs_username, df_init, 'initial', timestamps)

            df_final = create.final_df(df_init)

            write_data.df_to_json(hdfs_dir, yesterday, hdfs_username, df_final, 'final')

            write_data.insert_into_table(bookmark_file, hdfs_dir, spark, hdfs_username, df_final, 'final', timestamps)

            create.create_report(local_reports_dir, yesterday, df_final)

            unique_records(hdfs_dir, spark, hdfs_username)

            SparkSession.stop(spark)
