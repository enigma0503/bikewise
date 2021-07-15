import os


def mkdir_local(local_data_dir, ts_list):
    for timestamps in ts_list:
        os.popen(f'mkdir -p  {local_data_dir}/{timestamps[2]}')
        

def mkdir_hdfs(hdfs_dir, hdfs_username):
    os.popen(f'hdfs dfs -mkdir -p {hdfs_dir}/{hdfs_username}/bikewise/raw')
    os.popen(f'hdfs dfs -mkdir -p {hdfs_dir}/{hdfs_username}/bikewise/initial')
    os.popen(f'hdfs dfs -mkdir -p {hdfs_dir}/{hdfs_username}/bikewise/final')
    os.popen(f'hdfs dfs -mkdir -p {hdfs_dir}/{hdfs_username}/bikewise/final/incident_reports')