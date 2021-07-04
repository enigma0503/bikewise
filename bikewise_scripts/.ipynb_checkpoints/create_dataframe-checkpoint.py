from pyspark.sql.functions import *

def dataframe(spark, file_path):
    df_raw = spark. \
    read. \
    json(file_path)

    df_raw = df_raw. \
    withColumn('year', date_format(date_sub(current_date(), 1), 'yyyy')). \
    withColumn('month', date_format(date_sub(current_date(),1), 'MM')). \
    withColumn('day', date_format(date_sub(current_date(),1), 'dd'))
    return(df_raw)