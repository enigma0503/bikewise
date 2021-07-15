from bookmark import get_bookmark
from bookmark import update_bookmark
import os
import config


def copy_files(bookmark_file, hdfs_dir, local_data_dir, ts_list, hdfs_username):
    for timestamps in ts_list:
        if timestamps[3] == 1:
            os.popen(
                f'''hdfs dfs -copyFromLocal -f {local_data_dir}/{timestamps[2]} {hdfs_dir}/{hdfs_username}/bikewise/raw''')
            update_bookmark(bookmark_file, timestamps[2], 4)


if __name__ == "__main__":
    env = os.environ.get('ENVIRON')
    config_loc = os.environ.get('CONFIG_LOC')

    conf = config.get_config(config_loc, env)

    hdfs_username = conf['HDFS_USERNAME']
    bookmark_file = conf['BOOKMARK_FILE']
    local_data_dir = conf['LOCAL_DATA_DIR']
    hdfs_dir = conf['HDFS_DIR']
    local_reports_dir = conf['LOCAL_REPORTS_DIR']

    ts_list = get_bookmark(bookmark_file, 4)

    copy_files(bookmark_file, hdfs_dir, local_data_dir, ts_list, hdfs_username)
