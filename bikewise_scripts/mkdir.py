import os


def mkdir_local(ts_list):
    for timestamps in ts_list:
        os.popen(f'mkdir -p  ~/shubham/bike_data/{timestamps[2]}')
        

def mkdir_hdfs(hdfs_username):
    os.popen(f'hdfs dfs -mkdir -p /user/{hdfs_username}/bikewise/raw')
    os.popen(f'hdfs dfs -mkdir -p /user/{hdfs_username}/bikewise/initial')
    os.popen(f'hdfs dfs -mkdir -p /user/{hdfs_username}/bikewise/final')
    os.popen(f'hdfs dfs -mkdir -p /user/{hdfs_username}/bikewise/final/incident_reports')