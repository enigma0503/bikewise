import datetime
import time
import json
import requests
import os
import getpass
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, catalog

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# spark = None
username = getpass.getuser()

#get the timestamps to fectch the data from the API

def get_timestamp(**kwargs):
    current_ts = int(time.time())
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    to_time = datetime.datetime.strptime(today,"%Y-%m-%d")  
    today_ts = int(datetime.datetime.timestamp(to_time)) - 14400
    kwargs['ti'].xcom_push(key='timestamps', value = [today_ts, current_ts, today])
    
    
    
# just to log the results
    
def print_ts(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp',])
    data = ti.xcom_pull(key='data', task_ids=['get_response',])
    print("this is shubham" , pulled_value)
    print("api_data", data)
    
    
    
# get the response from the API
    
def get_response(**kwargs):
    
    ti = kwargs['ti']
    url = f'https://bikewise.org:443/api/v2/incidents'
    timestamps = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp',])
    header = {
      "Cache-Control": "max-age=0, private, must-revalidate",
      "Content-Type": "application/json"
    }
    
    parameters = { "page": 1,
                   "per_page": 10000,
                   "occurred_before": timestamps[0][1],
                   "occurred_after": timestamps[0][0]
              }
    try:
        response = requests.get(url, headers=header, params = parameters)
        data = response.json()
        data = data["incidents"]
        kwargs['ti'].xcom_push(key='data', value = data)
        print('response_data_bikewise', data)
    except Exception as e:
        print(e)

        
        
# create a JSON file for the received data

def create_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids=['get_response',])
    timestamps = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp',])

    for values in data[0]:
        with open(f'/home/itv000579/shubham/bike_data/{timestamps[0][2]}/{timestamps[0][2]}.json', 'a') as f:
            f.write(json.dumps(values) + '\n')

    
# airflow code below        

default_args = {
    'owner': 'enigma0503',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id = 'bikewise_api',
    default_args = default_args,
    schedule_interval = '0 0 * * *',
    catchup = False
)

# jobs below

get_timestamp = PythonOperator(
        task_id = 'get_timestamp',
        python_callable = get_timestamp,
        dag = dag
    )

get_response = PythonOperator(
        task_id = 'get_response',
        python_callable = get_response,
        dag = dag
    )

create_file = PythonOperator(
        task_id = 'create_file',
        python_callable = create_file,
        dag = dag
    )

print_ts = PythonOperator(
        task_id = 'print_ts',
        python_callable = print_ts,
        dag = dag
    )

create_bike_data_dir = BashOperator(
    task_id = 'create_bike_data_dir',
    bash_command = " mkdir -p ~/shubham/bike_data/`date '+%Y-%m-%d'` ",
    dag = dag
)

create_hdfs_raw_dir = BashOperator(
    task_id = 'create_hdfs_raw_dir',
    bash_command = 'hdfs dfs -mkdir -p /user/${USER}/bikewise/raw',
    dag = dag
)

create_hdfs_init_dir = BashOperator(
    task_id = 'create_hdfs_init_dir',
    bash_command = 'hdfs dfs -mkdir -p /user/${USER}/bikewise/initial',
    dag = dag
)

create_hdfs_final_dir = BashOperator(
    task_id = 'create_hdfs_final_dir',
    bash_command = 'hdfs dfs -mkdir -p /user/${USER}/bikewise/final',
    dag = dag
)


copy_json_file_to_hdfs = BashOperator(
    task_id = 'copy_json_file_to_hdfs',
    bash_command = "hdfs dfs -copyFromLocal /home/itv000579/shubham/bike_data/`date '+%Y-%m-%d'` /user/${USER}/bikewise/raw",
    dag = dag
)

spark_job = BashOperator(
    task_id = 'spark_job',
    bash_command = '/home/itv000579/airflow/airflow-env/bin/python /home/itv000579/airflow/dags/sparkjob.py',
    dag = dag
)

# relations below


create_hdfs_raw_dir >> copy_json_file_to_hdfs
create_hdfs_init_dir
create_hdfs_final_dir
# create_hdfs_checking_dir

create_bike_data_dir >> get_timestamp >>  get_response  >> print_ts >> create_file >> copy_json_file_to_hdfs
copy_json_file_to_hdfs >> spark_job

if __name__ == "__main__":
    dag.cli()
