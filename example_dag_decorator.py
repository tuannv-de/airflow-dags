from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import pendulum
import httpx


def get_request(**kwargs):
    url = kwargs['url']
    response = httpx.get(url).json()
    kwargs['ti'].xcom_push(key='origin_ip', value=response['origin'])


def prepare_command(**kwargs):
    origin_ip = kwargs['ti'].xcom_pull(key='origin_ip')
    return f"echo 'Seems like today your server executing Airflow is connected from IP {origin_ip}'"


with DAG(
    dag_id="example_dag_decorator_compat",
    schedule_interval="*/3 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    get_ip = PythonOperator(
        task_id="get_ip",
        python_callable=get_request,
        op_kwargs={"url": "http://httpbin.org/get"},
    )

    command_task = PythonOperator(
        task_id="prepare_command",
        python_callable=prepare_command,
    )

    echo_task = BashOperator(
        task_id="echo_ip_info",
        bash_command="{{ task_instance.xcom_pull(task_ids='prepare_command') }}",
    )

    get_ip >> command_task >> echo_task
