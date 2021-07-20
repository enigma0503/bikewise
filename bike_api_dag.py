from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

sshHook = SSHHook(ssh_conn_id='SSH_CONNECTION')

default_args = {
    'owner': 'enigma0503',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='bikewise_api_data',
    default_args=default_args,
    schedule_interval='30 0 * * *',
    catchup=False
)

python_loc = Variable.get('PYTHON_LOC')
scripts_dir = Variable.get('SCRIPTS_DIR')

download_data = SSHOperator(
    task_id='download_data',
    command=f'{python_loc}/python {scripts_dir}/downloadJob.py && sleep 10',
    ssh_hook=sshHook,
    dag=dag
)

copy_to_hdfs = SSHOperator(
    task_id='copy_to_hdfs',
    command=f'{python_loc}/python {scripts_dir}/hdfsJob.py && sleep 10',
    ssh_hook=sshHook,
    dag=dag
)

spark_job = SSHOperator(
    task_id='spark_job',
    command=f'{python_loc}/python {scripts_dir}/sparkJob.py && sleep 10',
    ssh_hook=sshHook,
    dag=dag
)

download_data >> copy_to_hdfs >> spark_job

if __name__ == "__main__":
    dag.cli()
