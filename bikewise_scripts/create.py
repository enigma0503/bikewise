from pyspark.sql.functions import *
from bookmark import update_bookmark
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def create_file(bookmark_file, local_data_dir, data, timestamps):
    for values in data:
        with open(f'{local_data_dir}/{timestamps[2]}/{timestamps[2]}.json', 'a') as f:
            f.write(json.dumps(values) + '\n')

    update_bookmark(bookmark_file, timestamps[2], 3)


def create_db(spark, username):
    spark.sql(f'create database if not exists {username}_bikewise_raw')
    spark.sql(f'create database if not exists {username}_bikewise_initial')
    spark.sql(f'create database if not exists {username}_bikewise_final')


def raw_df(spark, file_path, yesterday):
    ys_lst = yesterday.split('-')
    df_raw = spark. \
        read. \
        json(file_path)

    df_raw = df_raw. \
        withColumn('year', lit(ys_lst[0])). \
        withColumn('month', lit(ys_lst[1])). \
        withColumn('day', lit(ys_lst[2]))
    return df_raw


def init_df(df):
    df_init = df. \
        select('id', 'type', 'title', 'description', 'location_type',
               'location_description', 'media.image_url',
               'occurred_at', 'updated_at', 'type_properties',
               'year', 'month', 'day')

    return df_init


def final_df(df_init):
    df_final = df_init. \
        select('id', 'type', 'title', 'description',
               'location_description', 'image_url',
               'occurred_at', 'updated_at', 'year', 'month', 'day'). \
        withColumn('occurred_at', from_unixtime('occurred_at', "yyyy-MM-dd HH:mm:ss")). \
        withColumn('updated_at', from_unixtime('updated_at', "yyyy-MM-dd HH:mm:ss"))

    return df_final


def create_report(local_reports_dir, yesterday, df):
    df = df.select('type'). \
        groupBy(col('type')).count()

    df = df.toPandas()

    graph = plt.figure(figsize=(10, 8))
    splot = sns.barplot(x="type", y="count", data=df)
    for p in splot.patches:
        splot.annotate(format(p.get_height(), '.0f'),
                       (p.get_x() + p.get_width() / 2., p.get_height()),
                       ha='center', va='center',
                       xytext=(0, 9),
                       textcoords='offset points')
    plt.xlabel("Incident Type", size=14)
    plt.ylabel("Count", size=14)
    plt.title("Count of Incidents", size=20)
    plt.savefig(f'{local_reports_dir}/report_{yesterday}.pdf')
    plt.close()
