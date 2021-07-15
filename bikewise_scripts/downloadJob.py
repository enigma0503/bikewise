from timestamp import get_timestamp
import bookmark
from api_response import get_response
import mkdir


if __name__ == "__main__":
    
    hdfs_username = 'itv000579'
    
    timestamps = get_timestamp()
    
    bookmark.insert_bookmark(timestamps)
    
    ts_list = bookmark.get_bookmark(3)
    
    mkdir.mkdir_local(ts_list)
    
    mkdir.mkdir_hdfs(hdfs_username)
    
    get_response(ts_list)
    