def create_db(spark, username):
    spark.sql(f'create database if not exists {username}_bikewise_raw')
    spark.sql(f'create database if not exists {username}_bikewise_initial')
    spark.sql(f'create database if not exists {username}_bikewise_final')