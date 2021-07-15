import datetime
import time


def get_timestamp():
    current_ts = int(time.time())
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    today = today.strftime("%Y-%m-%d")
    yesterday = yesterday.strftime("%Y-%m-%d")
    to_time = datetime.datetime.strptime(today,"%Y-%m-%d")  
    today_ts = int(datetime.datetime.timestamp(to_time)) - 14400
    yesterday_ts = today_ts - 86400
    return (yesterday_ts ,today_ts, yesterday,1,0,0)