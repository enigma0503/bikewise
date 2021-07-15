from bookmark import get_bookmark
from bookmark import update_bookmark
import os



def copy_files(ts_list, hdfs_username):
    for timestamps in ts_list:
        if(timestamps[3]==1):
            os.popen(f'''hdfs dfs -copyFromLocal -f ~/shubham/bike_data/{timestamps[2]}   /user/{hdfs_username}/bikewise/raw''')
            update_bookmark(timestamps[2], 4)
    

if __name__ == "__main__":
    
    hdfs_username = 'itv000579'
    
    ts_list = get_bookmark(4)
    
    copy_files(ts_list, hdfs_username)
    
    