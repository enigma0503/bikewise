from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog

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
        