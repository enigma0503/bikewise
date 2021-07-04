def df_to_json(yesterday, hdfs_username, df, table_name):
    df.write.mode('overwrite').format('json').save(f'/user/{hdfs_username}/bikewise/{table_name}/{yesterday}')        