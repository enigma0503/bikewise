from timestamp import get_timestamp
import bookmark
from api_response import get_response
import mkdir
import config
import os

if __name__ == "__main__":
    env = os.environ.get('ENVIRON')
    config_loc = os.environ.get('CONFIG_LOC')
    print("ENVIRONMENT IN USE = ", env)
    print("LOCATION OF CONFIG FILE = ", config_loc)

    conf = config.get_config(config_loc, env)

    url = conf['URL']
    hdfs_username = conf['HDFS_USERNAME']
    bookmark_file = conf['BOOKMARK_FILE']
    local_data_dir = conf['LOCAL_DATA_DIR']
    hdfs_dir = conf['HDFS_DIR']
    local_reports_dir = conf['LOCAL_REPORTS_DIR']

    timestamps = get_timestamp()

    bookmark.insert_bookmark(bookmark_file, timestamps)

    ts_list = bookmark.get_bookmark(bookmark_file, 3)

    mkdir.mkdir_local(local_data_dir, ts_list)

    mkdir.mkdir_hdfs(hdfs_dir, hdfs_username)

    get_response(bookmark_file, local_data_dir, ts_list, url)
