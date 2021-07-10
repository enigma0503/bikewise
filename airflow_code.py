import datetime
import time
import json
import requests
import os
import getpass
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago

username = getpass.getuser()


# get the timestamps to fetch the data from the API

def get_timestamp(**kwargs):
    current_ts = int(time.time())
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    today = today.strftime("%Y-%m-%d")
    yesterday = yesterday.strftime("%Y-%m-%d")
    to_time = datetime.datetime.strptime(today, "%Y-%m-%d")
    today_ts = int(datetime.datetime.timestamp(to_time)) - 14400
    yesterday_ts = today_ts - 86400
    kwargs['ti'].xcom_push(key='timestamps', value=[yesterday_ts, today_ts, yesterday])


# just to log the results

def print_ts(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp', ])
    data = ti.xcom_pull(key='data', task_ids=['get_response', ])
    print("this is shubham", pulled_value)
    print("api_data", data)


# get the response from the API

def get_response(**kwargs):
    ti = kwargs['ti']
    url = f'https://bikewise.org:443/api/v2/incidents'
    timestamps = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp', ])
    header = {
        "Cache-Control": "max-age=0, private, must-revalidate",
        "Content-Type": "application/json"
    }

    parameters = {"page": 1,
                  "per_page": 10000,
                  "occurred_before": timestamps[0][1],
                  "occurred_after": timestamps[0][0]
                  }
    response = requests.get(url, headers=header, params=parameters)
    data = response.json()
    data = data["incidents"]
    # print("this is to fail the node", data[0]['id'])
    if len(data) > 0:
        kwargs['ti'].xcom_push(key='data', value=data)
        print('response_data_bikewise', data)
        return True
    else:
        print('Data received from API is: ', data)
        return False


# create a JSON file for the received data

def create_file(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids=['get_response', ])
    timestamps = ti.xcom_pull(key='timestamps', task_ids=['get_timestamp', ])

    for values in data[0]:
        with open(f'/home/itv000579/shubham/bike_data/{timestamps[0][2]}/{timestamps[0][2]}.json', 'a') as f:
            f.write(json.dumps(values) + '\n')


# airflow code below        

default_args = {
    'owner': 'enigma0503',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='bikewise_api',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False
)

# jobs below

get_timestamp = PythonOperator(
    task_id='get_timestamp',
    python_callable=get_timestamp,
    dag=dag
)

get_response = ShortCircuitOperator(
    task_id='get_response',
    python_callable=get_response,
    dag=dag
)
# get_response = PythonOperator(
#     task_id='get_response',
#     python_callable=get_response,
#     dag=dag
# )

create_file = PythonOperator(
    task_id='create_file',
    python_callable=create_file,
    dag=dag
)

print_ts = PythonOperator(
    task_id='print_ts',
    python_callable=print_ts,
    dag=dag
)

create_bike_data_dir = BashOperator(
    task_id='create_bike_data_dir',
    bash_command='mkdir -p ~/shubham/bike_data/`date -d "1 days ago" +"%Y-%m-%d"`',
    dag=dag
)

create_hdfs_raw_dir = BashOperator(
    task_id='create_hdfs_raw_dir',
    bash_command='hdfs dfs -mkdir -p /user/${USER}/bikewise/raw',
    dag=dag
)

create_hdfs_init_dir = BashOperator(
    task_id='create_hdfs_init_dir',
    bash_command='hdfs dfs -mkdir -p /user/${USER}/bikewise/initial',
    dag=dag
)

create_hdfs_final_dir = BashOperator(
    task_id='create_hdfs_final_dir',
    bash_command='hdfs dfs -mkdir -p /user/${USER}/bikewise/final',
    dag=dag
)

create_hdfs_reports_dir = BashOperator(
    task_id='create_hdfs_reports_dir',
    bash_command='hdfs dfs -mkdir -p /user/${USER}/bikewise/final/reports',
    dag=dag
)

sleep = BashOperator(
    task_id='sleep',
    bash_command='sleep 20',
    dag=dag
)

copy_json_file_to_hdfs = BashOperator(
    task_id='copy_json_file_to_hdfs',
    bash_command='hdfs dfs -copyFromLocal -f /home/itv000579/shubham/bike_data/`date -d "1 days ago" +"%Y-%m-%d"` /user/${USER}/bikewise/raw',
    dag=dag
)

spark_job = BashOperator(
    task_id='spark_job',
    bash_command='/home/itv000579/airflow/airflow-env/bin/python /home/itv000579/airflow/bikewise_scripts/sparkjob.py && sleep 60',
    dag=dag
)

copy_report = BashOperator(
    task_id='copy_report',
    bash_command='hdfs dfs -copyFromLocal -f /home/itv000579/shubham/bike_data/reports/report_`date -d "1 days ago" +"%Y-%m-%d"`.pdf /user/${USER}/bikewise/final/reports',
    dag=dag
)

# relations below

create_bike_data_dir >> get_timestamp >> get_response >> print_ts >> create_file >> sleep >> copy_json_file_to_hdfs >> spark_job
get_response >> create_hdfs_raw_dir >> copy_json_file_to_hdfs
get_response >> create_hdfs_init_dir >> spark_job
get_response >> create_hdfs_final_dir >> create_hdfs_reports_dir >> spark_job >> copy_report
copy_json_file_to_hdfs >> spark_job

if __name__ == "__main__":
    dag.cli()
