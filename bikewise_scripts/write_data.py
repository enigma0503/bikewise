from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog
from bookmark import update_bookmark


def df_to_json(hdfs_dir, yesterday, hdfs_username, df, table_name):
    df.write.mode('overwrite').format('json').save(f'{hdfs_dir}/{hdfs_username}/bikewise/{table_name}/{yesterday}')


def insert_into_table(bookmark_file, hdfs_dir, spark, hdfs_username, df, table_name, timestamps):
    tables = spark.catalog.listTables(f'{hdfs_username}_bikewise_{table_name}')
    found = False
    for table in tables:
        if list(table)[0] == f'incidents_{table_name}':
            found = True
            break
    if found:
        df. \
            write. \
            mode('append'). \
            partitionBy('year', 'month', 'day'). \
            parquet(f'{hdfs_dir}/{hdfs_username}/warehouse/{hdfs_username}_bikewise_{table_name}.db/incidents_{table_name}')

        spark.sql(f'''MSCK REPAIR TABLE 
              {hdfs_username}_bikewise_{table_name}.incidents_{table_name}''')
    else:
        df. \
            write. \
            partitionBy('year', 'month', 'day'). \
            saveAsTable(f'{hdfs_username}_bikewise_{table_name}.incidents_{table_name}')

    if table_name == 'final':
        update_bookmark(bookmark_file, timestamps[2], 5)
